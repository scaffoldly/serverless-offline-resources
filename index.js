"use strict";
const _ = require("lodash");
const BbPromise = require("bluebird");
const AWS = require("aws-sdk");

const LOCALSTACK_ENDPOINT = "http://localhost.localstack.cloud:4566";

class ServerlessOfflineResources {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.service = serverless.service;
    this.config =
      (this.service.custom && this.service.custom["offline-resources"]) || {};

    this.options = options;
    this.provider = "aws";

    this.commands = {};

    this.hooks = {
      "before:offline:start": this.startHandler.bind(this),
      //   "before:offline:start:end": this.endHandler.bind(this),
    };
  }

  get endpoint() {
    const config =
      (this.service.custom && this.service.custom["offline-resources"]) || {};
    return _.get(config, "endpoint", LOCALSTACK_ENDPOINT);
  }

  get region() {
    const config =
      (this.service.custom && this.service.custom["offline-resources"]) || {};
    return _.get(config, "region", "us-east-1");
  }

  get accessKeyId() {
    const config =
      (this.service.custom && this.service.custom["offline-resources"]) || {};
    const val = _.get(config, "accessKeyId", undefined);

    if (!val && this.endpoint === LOCALSTACK_ENDPOINT) {
      return "test";
    }

    return val;
  }

  get secretAccessKey() {
    const config =
      (this.service.custom && this.service.custom["offline-resources"]) || {};
    const val = _.get(config, "secretAccessKey", undefined);

    if (!val && this.endpoint === LOCALSTACK_ENDPOINT) {
      return "test";
    }

    return val;
  }

  get stage() {
    return (
      (this.options && this.options.stage) ||
      (this.service.provider && this.service.provider.stage)
    );
  }

  shouldExecute() {
    if (this.config.stages && this.config.stages.includes(this.stage)) {
      return true;
    }
    return false;
  }

  async startHandler() {
    if (this.shouldExecute()) {
      await this.dynamoDbHandler();
    }
  }

  async endHandler() {
    if (this.shouldExecute()) {
      //   console.log(`Offline Resources is ending for stage: ${this.stage}`);
    }
  }

  getDefaultStack() {
    return _.get(this.service, "resources");
  }

  getResourceDefinitionsFromStack(stack, name) {
    const resources = _.get(stack, "Resources", []);
    return Object.keys(resources)
      .map((key) => {
        if (resources[key].Type === name) {
          return {
            __key: key,
            ...resources[key].Properties,
          };
        }
      })
      .filter((n) => n);
  }

  //
  // DynamoDB
  //

  async dynamoDbHandler() {
    if (this.shouldExecute()) {
      const dynamodb = this.dynamodbOptions();
      const tables = this.tables;
      await Promise.all(
        tables.map(async (table) => {
          const definition = await this.createDynamoDbTable(dynamodb, table);
          await this.createDynamoDbStreams(dynamodb, definition);
        })
      );
    }
  }

  get tables() {
    let stacks = [];

    const defaultStack = this.getDefaultStack();
    if (defaultStack) {
      stacks.push(defaultStack);
    }

    return stacks
      .map((stack) =>
        this.getResourceDefinitionsFromStack(stack, "AWS::DynamoDB::Table")
      )
      .reduce((tables, tablesInStack) => tables.concat(tablesInStack), []);
  }

  dynamodbOptions() {
    let dynamoOptions = {
      endpoint: this.endpoint,
      region: this.region,
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
    };

    return {
      raw: new AWS.DynamoDB(dynamoOptions),
      doc: new AWS.DynamoDB.DocumentClient(dynamoOptions),
    };
  }

  async createDynamoDbTable(dynamodb, migration) {
    const key = migration.__key;
    delete migration.__key;

    if (
      migration.StreamSpecification &&
      migration.StreamSpecification.StreamViewType
    ) {
      migration.StreamSpecification.StreamEnabled = true;
    }
    if (migration.TimeToLiveSpecification) {
      delete migration.TimeToLiveSpecification;
    }
    if (migration.SSESpecification) {
      migration.SSESpecification.Enabled =
        migration.SSESpecification.SSEEnabled;
      delete migration.SSESpecification.SSEEnabled;
    }
    if (migration.PointInTimeRecoverySpecification) {
      delete migration.PointInTimeRecoverySpecification;
    }
    if (migration.Tags) {
      delete migration.Tags;
    }
    if (migration.BillingMode === "PAY_PER_REQUEST") {
      delete migration.BillingMode;

      const defaultProvisioning = {
        ReadCapacityUnits: 5,
        WriteCapacityUnits: 5,
      };
      migration.ProvisionedThroughput = defaultProvisioning;
      if (migration.GlobalSecondaryIndexes) {
        migration.GlobalSecondaryIndexes.forEach((gsi) => {
          gsi.ProvisionedThroughput = defaultProvisioning;
        });
      }
    }

    try {
      const definition = await dynamodb.raw.createTable(migration).promise();
      console.log(
        `[offline-resources][dynamodb][${key}] Table created: ${migration.TableName}`
      );
      console.log("!!! create table", definition);
      return definition.TableDescription;
    } catch (err) {
      if (err.name === "ResourceInUseException") {
        console.log(
          `[offline-resources][dynamodb][${key}] Table exists: ${migration.TableName}`
        );
        const definition = await dynamodb.raw
          .describeTable({ TableName: migration.TableName })
          .promise();

        console.log("!!! describe table", definition);
        return definition.Table;
      } else {
        console.warn(
          `[offline-resources][dynamodb][${key}] Unable to create table: ${migration.TableName} - ${err.message}`
        );
        throw err;
      }
    }
  }

  async createDynamoDbStreams(dynamodb, definition) {
    console.log("!!! create stream", definition);
  }
}
module.exports = ServerlessOfflineResources;
