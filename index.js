"use strict";
const _ = require("lodash");
const BbPromise = require("bluebird");
const AWS = require("aws-sdk");
const { DynamoDBStreamsClient } = require("@aws-sdk/client-dynamodb-streams");
const { DynamoDBStreamPoller, StreamEvent } = require("./streams");
const { LambdaClient, InvokeCommand } = require("@aws-sdk/client-lambda");

const LOCALSTACK_ENDPOINT = "http://localhost.localstack.cloud:4566";

class ServerlessOfflineResources {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.service = serverless.service;
    this.config =
      (this.service.custom && this.service.custom["offline-resources"]) || {};

    this.dynamoDbPoller = null;

    this.options = options;
    this.provider = "aws";

    this.commands = {};

    this.hooks = {
      "before:offline:start": this.startHandler.bind(this),
      "before:offline:start:end": this.endHandler.bind(this),
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
      // await this.dynamoDbHandler();
      await this.resourcesHandler();
    }
  }

  async endHandler() {
    if (this.shouldExecute()) {
      console.log(`Offline Resources is ending for stage: ${this.stage}`);
      if (this.dynamoDbPoller) {
        this.dynamoDbPoller.stop();
      }
    }
  }

  getDefaultStack() {
    return _.get(this.service, "resources");
  }

  getResources() {
    return _.get(this.service, "resources", {});
  }

  getResourceDefinitionsFromStack(stack, names) {
    const resources = _.get(stack, "Resources", []);
    return Object.keys(resources)
      .map((key) => {
        if (names.includes(resources[key].Type)) {
          return {
            __key: key,
            ...resources[key].Properties,
          };
        }
      })
      .filter((n) => n);
  }

  getFunctionsWithStreamEvent(type, key) {
    return this.service.getAllFunctions().reduce((acc, functionName) => {
      const functionObject = this.service.getFunction(functionName);
      // find functions with events with "stream" and type "dynamodb"
      const event = functionObject.events.find((event) => {
        if (
          event.stream &&
          event.stream.type === type &&
          event.stream.arn &&
          event.stream.arn["Fn::GetAtt"] &&
          event.stream.arn["Fn::GetAtt"][0] === key &&
          event.stream.arn["Fn::GetAtt"][1] === "StreamArn"
        ) {
          return true;
        }
      });

      if (!event) {
        return acc;
      }

      acc.push({
        functionName: functionObject.name,
        // TODO Slice on error and other properties
        batchSize: event.batchSize || 1,
        maximumRecordAgeInSeconds: event.maximumRecordAgeInSeconds || undefined,
        recordStreamHandler: this.emitStreamRecords.bind(this),
      });

      return acc;
    }, []);
  }

  //
  // DynamoDB
  //

  async resourcesHandler() {
    const clients = this.clients();
    // const resources = this.resources;

    try {
      const stackName = `${this.service.service}-${this.stage}`;

      console.log(
        `[offline-resources][cloudformation][${stackName}] Creating stack.`
      );
      const stack = await clients.cloudformation
        .createStack({
          StackName: stackName,
          Capabilities: ["CAPABILITY_IAM"],
          OnFailure: "DELETE",
          Parameters: [],
          Tags: [],
          TemplateBody: JSON.stringify(this.getResources()),
        })
        .promise();

      console.log("!!! create stack", stack);
    } catch (createErr) {
      if (createErr.name !== "ValidationError") {
        console.warn(
          `[offline-resources][cloudformation] Unable to create stack. - ${createErr.message}`
        );
        throw err;
      }

      try {
        console.log(
          `[offline-resources][cloudformation][${stackName}] Updating stack.`
        );
        const stack = await clients.cloudformation
          .updateStack({
            StackName: stackName,
            Capabilities: ["CAPABILITY_IAM"],
            OnFailure: "DELETE",
            Parameters: [],
            Tags: [],
            TemplateBody: JSON.stringify(this.getResources()),
          })
          .promise();

        console.log("!!! update stack", stack);
      } catch (updateErr) {
        console.warn(
          `[offline-resources][cloudformation] Unable to update stack. - ${updateErr.message}`
        );
        throw err;
      }
    }
  }

  async dynamoDbHandler() {
    if (this.shouldExecute()) {
      const dynamodb = this.dynamodbOptions();
      const tables = this.tables;
      const resources = this.resources;

      console.log("!!! resources", resources);

      await Promise.all(
        tables.map(async (table) => {
          const definition = await this.createDynamoDbTable(dynamodb, table);
          const functions = this.getFunctionsWithStreamEvent(
            "dynamodb",
            definition.key
          );
          await this.createDynamoDbStreams(dynamodb, definition, functions);
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

  get resources() {
    let stacks = [];

    const defaultStack = this.getDefaultStack();
    if (defaultStack) {
      stacks.push(defaultStack);
    }

    return stacks
      .map((stack) =>
        this.getResourceDefinitionsFromStack(stack, [
          "AWS::DynamoDB::Table",
          "AWS::SNS::Topic",
          "AWS::SQS::Queue",
        ])
      )
      .reduce((resources, resourcesInStack) =>
        resources.concat(resourcesInStack)
      );
  }

  clients() {
    let options = {
      endpoint: this.endpoint,
      region: this.region,
      accessKeyId: this.accessKeyId,
      secretAccessKey: this.secretAccessKey,
    };

    return {
      cloudformation: new AWS.CloudFormation(options),
      dynamodb: new AWS.DynamoDB(options),
      dynamodbstreams: new AWS.DynamoDBStreams(options),
      sns: new AWS.SNS(options),
      sqs: new AWS.SQS(options),
    };
  }

  // async createResources() {
  //   const clients = this.clients();
  //   const resources = this.resources;
  //   await Promise.all(
  //     resources.map(async (resource) => {
  //       const key = resource.__key;
  //       delete resource.__key;

  //       try {
  //         await clients.cloudformation
  //           .createStack({
  //             StackName: `${this.service.service}-${this.stage}-${key}`,
  //             Capabilities: ["CAPABILITY_IAM"],
  //             OnFailure: "DELETE",
  //             Parameters: [],
  //             Tags: [],
  //             TemplateBody: JSON.stringify({
  //               Resources: {
  //                 [key]: resource,
  //               },
  //             }),
  //           })
  //           .promise();
  //         console.log(
  //           `[offline-resources][${key}] Resource created: ${resource.Type}`
  //         );
  //       } catch (err) {
  //         if (err.name === "AlreadyExistsException") {
  //           console.log(
  //             `[offline-resources][${key}] Resource exists: ${resource.Type}`
  //           );
  //         } else {
  //           console.warn(
  //             `[offline-resources][${key}] Unable to create resource: ${resource.Type} - ${err.message}`
  //           );
  //           throw err;
  //         }
  //       }
  //     })
  //   );
  // }

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
      stream: new DynamoDBStreamsClient({
        region: dynamoOptions.region,
        endpoint: dynamoOptions.endpoint,
        credentials: {
          accessKeyId: dynamoOptions.accessKeyId,
          secretAccessKey: dynamoOptions.secretAccessKey,
        },
        maxAttempts: 1,
      }),
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
      return {
        key,
        name: definition.TableDescription.TableName,
        arn: definition.TableDescription.TableArn,
        streamArn: definition.TableDescription.LatestStreamArn,
      };
    } catch (err) {
      if (err.name === "ResourceInUseException") {
        console.log(
          `[offline-resources][dynamodb][${key}] Table exists: ${migration.TableName}`
        );
        const definition = await dynamodb.raw
          .describeTable({ TableName: migration.TableName })
          .promise();

        return {
          key,
          name: definition.Table.TableName,
          arn: definition.Table.TableArn,
          streamArn: definition.Table.LatestStreamArn,
        };
      } else {
        console.warn(
          `[offline-resources][dynamodb][${key}] Unable to create table: ${migration.TableName} - ${err.message}`
        );
        throw err;
      }
    }
  }

  async createDynamoDbStreams(dynamodb, definition, functions) {
    this.dynamoDbPoller = new DynamoDBStreamPoller(
      dynamodb.stream,
      definition.streamArn,
      functions
    );

    await this.dynamoDbPoller.start();
  }

  async emitStreamRecords(records, functionName, streamArn) {
    if (!records || !records.length) {
      return;
    }
    const client = new LambdaClient({
      region: "us-east-1",
      apiVersion: "2015-03-31",
      endpoint: "http://localhost:3002",
    });
    const event = new StreamEvent(records, this.region, streamArn);
    try {
      client.send(
        new InvokeCommand({
          FunctionName: functionName,
          Payload: JSON.stringify(event),
          InvocationType: "Event",
        })
      );
    } catch (e) {
      console.warn("Error invoking", e);
    }
  }
}
module.exports = ServerlessOfflineResources;
