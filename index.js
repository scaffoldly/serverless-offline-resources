"use strict";
const _ = require("lodash");
const BbPromise = require("bluebird");
const AWS = require("aws-sdk");

class ServerlessOfflineResources {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.service = serverless.service;
    this.serverlessLog = serverless.cli.log.bind(serverless.cli);
    this.config =
      (this.service.custom && this.service.custom.offlineResources) || {};
    this.options = options;
    this.provider = "aws";

    this.commands = {};

    this.hooks = {
      "before:offline:start:init": this.startHandler.bind(this),
      "before:offline:start:end": this.endHandler.bind(this),
    };
  }

  get endpoint() {
    const config =
      (this.service.custom && this.service.custom.offlineResources) || {};
    const port = _.get(config, "endpoint", "http://localhost:4566");
    return port;
  }

  get region() {
    const config =
      (this.service.custom && this.service.custom.offlineResources) || {};
    const port = _.get(config, "region", "us-east-1");
    return port;
  }

  get accessKeyId() {
    const config =
      (this.service.custom && this.service.custom.offlineResources) || {};
    const port = _.get(config, "accessKeyId", "test");
    return port;
  }

  get secretAccessKey() {
    const config =
      (this.service.custom && this.service.custom.offlineResources) || {};
    const port = _.get(config, "secretAccessKey", "test");
    return port;
  }

  get stage() {
    return (
      (this.options && this.options.stage) ||
      (this.service.provider && this.service.provider.stage)
    );
  }

  shouldExecute() {
    let hasServerlessOffline = _.get(this.service, "plugins", []).includes(
      "serverless-offline"
    );
    if (
      hasServerlessOffline &&
      this.config.stages &&
      this.config.stages.includes(this.stage)
    ) {
      return true;
    }
    return false;
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

  dynamoDbHandler() {
    if (this.shouldExecute()) {
      const dynamodb = this.dynamodbOptions();
      const tables = this.tables;
      return BbPromise.each(tables, (table) =>
        this.createDynamoDbTable(dynamodb, table)
      );
    } else {
      this.serverlessLog(
        "Skipping migration: DynamoDB Table Creation is not available for stage: " +
          this.stage
      );
    }
  }

  startHandler() {
    if (this.shouldExecute()) {
      // TODO Create Option
      return BbPromise.resolve().then(() => this.dynamoDbHandler());
    } else {
      this.serverlessLog(
        "Skipping start: Offline Resources is not available for stage: " +
          this.stage
      );
    }
  }

  endHandler() {
    if (this.shouldExecute()) {
      // TODO Cleanup Option
    } else {
      this.serverlessLog(
        "Skipping end: Offline Resources is not available for stage: " +
          this.stage
      );
    }
  }

  getDefaultStack() {
    return _.get(this.service, "resources");
  }

  getDynamoDbTableDefinitionsFromStack(stack) {
    const resources = _.get(stack, "Resources", []);
    return Object.keys(resources)
      .map((key) => {
        if (resources[key].Type === "AWS::DynamoDB::Table") {
          return resources[key].Properties;
        }
      })
      .filter((n) => n);
  }

  get tables() {
    let stacks = [];

    const defaultStack = this.getDefaultStack();
    if (defaultStack) {
      stacks.push(defaultStack);
    }

    return stacks
      .map((stack) => this.getDynamoDbTableDefinitionsFromStack(stack))
      .reduce((tables, tablesInStack) => tables.concat(tablesInStack), []);
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
            this.serverlessLog(
              `DynamoDB - Warn - table ${migration.TableName} already exists`
            );
            resolve();
          } else {
            this.serverlessLog("DynamoDB - Error - ", err);
            reject(err);
          }
        } else {
          this.serverlessLog("DynamoDB - created table " + migration.TableName);
          resolve(migration);
        }
      });
    });
  }
}
module.exports = ServerlessOfflineResources;
