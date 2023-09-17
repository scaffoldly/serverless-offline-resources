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

  startHandler() {
    if (this.shouldExecute()) {
      return BbPromise.resolve().then(() => this.dynamoDbHandler());
    }
  }

  endHandler() {
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
          return resources[key].Properties;
        }
      })
      .filter((n) => n);
  }

  //
  // DynamoDB
  //

  dynamoDbHandler() {
    if (this.shouldExecute()) {
      const dynamodb = this.dynamodbOptions();
      const tables = this.tables;
      return BbPromise.each(tables, (table) =>
        this.createDynamoDbTable(dynamodb, table)
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

  createDynamoDbTable(dynamodb, migration) {
    return new BbPromise((resolve, reject) => {
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
      dynamodb.raw.createTable(migration, (err) => {
        if (err) {
          if (err.name === "ResourceInUseException") {
            console.warn(
              `Offline Resources: DynamoDB Table exists: ${migration.TableName}`
            );
            resolve();
          } else {
            console.warn(`Offline Resources: DynamoDB Error:`, err);
            reject(err);
          }
        } else {
          console.log(
            `Offline Resources: DynamoDB Table created: ${migration.TableName}`
          );
          resolve(migration);
        }
      });
    });
  }
}
module.exports = ServerlessOfflineResources;
