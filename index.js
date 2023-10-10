"use strict";
const _ = require("lodash");
const BbPromise = require("bluebird");
const AWS = require("aws-sdk");
const { DynamoDBStreamsClient } = require("@aws-sdk/client-dynamodb-streams");
const { DynamoDBStreamPoller, StreamEvent } = require("./dynamodb-stream-poller");
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
      const resources = await this.resourcesHandler();
      await this.dynamoDbHandler(resources["AWS::DynamoDB::Table"]);
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

  getResources() {
    return _.get(this.service, "resources", {});
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

  async resourcesHandler() {
    let stackResources = {
      "AWS::DynamoDB::Table": [],
      "AWS::SNS::Topic": [],
      "AWS::SQS::Queue": [],
    };

    const clients = this.clients();
    // const resources = this.resources;
    const stackName = `${this.service.service}-${this.stage}`;

    try {
      console.log(
        `[offline-resources][cloudformation][${stackName}] Creating stack.`
      );
      await clients.cloudformation
        .createStack({
          StackName: stackName,
          Capabilities: ["CAPABILITY_IAM"],
          OnFailure: "DELETE",
          Parameters: [],
          Tags: [],
          TemplateBody: JSON.stringify(this.getResources()),
        })
        .promise();

      await clients.cloudformation
        .waitFor("stackCreateComplete", {
          StackName: stackName,
          $waiter: {
            delay: 1,
            maxAttempts: 60,
          },
        })
        .promise();
      console.log(
        `[offline-resources][cloudformation][${stackName}] Stack created.`
      );
    } catch (createErr) {
      if (createErr.name !== "ValidationError") {
        console.warn(
          `[offline-resources][cloudformation] Unable to create stack - ${createErr.message}`
        );
        throw createErr;
      }

      try {
        console.log(
          `[offline-resources][cloudformation][${stackName}] Stack already exists. Updating stack.`
        );
        await clients.cloudformation
          .updateStack({
            StackName: stackName,
            Capabilities: ["CAPABILITY_IAM"],
            Parameters: [],
            Tags: [],
            TemplateBody: JSON.stringify(this.getResources()),
          })
          .promise();

        await clients.cloudformation
          .waitFor("stackUpdateComplete", {
            StackName: stackName,
            $waiter: {
              delay: 1,
              maxAttempts: 60,
            },
          })
          .promise();

        console.log(
          `[offline-resources][cloudformation][${stackName}] Stack updated.`
        );
      } catch (updateErr) {
        console.warn(
          `[offline-resources][cloudformation] Unable to update stack - ${updateErr.message}`
        );
        throw updateErr;
      }
    }

    try {
      const stackResourcesResponse = await clients.cloudformation
        .listStackResources({
          StackName: stackName,
        })
        .promise();

      const supportedResources =
        stackResourcesResponse.StackResourceSummaries.forEach((r) => {
          if (Object.keys(stackResources).includes(r.ResourceType)) {
            stackResources[r.ResourceType].push({
              key: r.LogicalResourceId,
              id: r.PhysicalResourceId,
            });
          }
        });
    } catch (err) {
      console.warn(
        `[offline-resources][cloudformation] Unable to list stack resources - ${updateErr.message}`
      );
      throw err;
    }

    return stackResources;
  }

  async dynamoDbHandler(tables) {
    await Promise.all(
      tables.map(async (table) => {
        await this.createDynamoDbStreams(table.key, table.id);
      })
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
      dynamodbstreams: new DynamoDBStreamsClient(options),
      sns: new AWS.SNS(options),
      sqs: new AWS.SQS(options),
    };
  }

  async createDynamoDbStreams(tableKey, tableName) {
    const functions = this.getFunctionsWithStreamEvent("dynamodb", tableKey);

    const clients = this.clients();
    const table = await clients.dynamodb
      .describeTable({
        TableName: tableName,
      })
      .promise();

    const streamArn = table.Table.LatestStreamArn;

    console.warn(
      `[offline-resources][dynamodb][${tableKey}] Streaming to functions: ${functions.map(
        (f) => f.functionName
      )}`
    );

    this.dynamoDbPoller = new DynamoDBStreamPoller(
      clients.dynamodbstreams,
      streamArn,
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
