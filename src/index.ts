import _ from "lodash";
import AWS from "aws-sdk";
import {
  DynamoDBStreamsClient,
  _Record,
} from "@aws-sdk/client-dynamodb-streams";
import {
  DynamoDBStreamPoller,
  DynamoDbFunctionDefinition,
  MappedDynamoDBStreamEvent,
} from "./dynamodb-stream-poller";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { Message } from "@aws-sdk/client-sqs";
import {
  MappedSQSEvent,
  SqsFunctionDefinition,
  SqsQueuePoller,
  convertUrlToQueueName,
} from "./sqs-queue-poller";
import { SQSClient } from "@aws-sdk/client-sqs";

export const LOCALSTACK_ENDPOINT = "http://localhost.localstack.cloud:4566";
const PLUGIN_NAME = "offline-resources";

type SupportedResources =
  | "AWS::DynamoDB::Table"
  | "AWS::SNS::Topic"
  | "AWS::SQS::Queue";

type StackResources = { [key in SupportedResources]: StackResource[] };

type OfflineResourcesProps = {
  endpoint?: string;
  region?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  stages?: string[];
};

type StackResource = {
  key: string;
  id: string;
};

type ServerlessCustom = {
  "offline-resources"?: OfflineResourcesProps;
};

type ServerlessService = {
  service: string;
  custom?: ServerlessCustom;
  provider: {
    stage: string;
  };
  resources: any;
  getAllFunctions: () => string[];
  getFunction: (functionName: string) => any;
};

type Serverless = {
  service: ServerlessService;
};

type Options = {
  stage: string;
};

export const msg = (
  fn: (message?: any, ...optionalParams: any[]) => void,
  stage: string,
  message: string,
  obj?: any
) => {
  if (!message.startsWith("[")) {
    message = `[${PLUGIN_NAME}][${stage}] ${message}`;
  } else {
    message = `[${PLUGIN_NAME}][${stage}]${message}`;
  }
  if (obj) {
    if (obj instanceof Error) {
      fn(message, obj.message);
    } else {
      fn(message, JSON.stringify(obj));
    }
  } else {
    fn(message);
  }
};

class ServerlessOfflineResources {
  service: ServerlessService;
  config: OfflineResourcesProps;
  provider: "aws";
  hooks: {
    "before:offline:start": () => void;
    "before:offline:start:end": () => void;
  };

  dynamoDbPoller?: DynamoDBStreamPoller;
  sqsQueuePoller?: SqsQueuePoller;

  constructor(serverless: Serverless, private options: Options) {
    this.service = serverless.service;
    this.config =
      (this.service.custom && this.service.custom[PLUGIN_NAME]) || {};

    this.options = options;
    this.provider = "aws";

    this.hooks = {
      "before:offline:start": this.startHandler.bind(this),
      "before:offline:start:end": this.endHandler.bind(this),
    };
  }

  log(message: string, obj?: any) {
    msg(console.log, this.stage, message, obj);
  }

  warn(message: string, obj?: any) {
    msg(console.warn, this.stage, message, obj);
  }

  get endpoint() {
    const config =
      (this.service.custom && this.service.custom[PLUGIN_NAME]) || {};
    return _.get(config, "endpoint", LOCALSTACK_ENDPOINT);
  }

  get region() {
    const config =
      (this.service.custom && this.service.custom[PLUGIN_NAME]) || {};
    return _.get(config, "region", "us-east-1");
  }

  get accessKeyId() {
    const config =
      (this.service.custom && this.service.custom[PLUGIN_NAME]) || {};
    const val = _.get(config, "accessKeyId", undefined);

    if (!val && this.endpoint === LOCALSTACK_ENDPOINT) {
      return "test";
    }

    return val;
  }

  get secretAccessKey() {
    const config =
      (this.service.custom && this.service.custom[PLUGIN_NAME]) || {};
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
      this.log(`Starting...`);
      const resources = await this.resourcesHandler();
      console.log("!!! resources", JSON.stringify(resources, null, 2));
      await this.dynamoDbHandler(resources["AWS::DynamoDB::Table"]);
      await this.sqsHandler(resources["AWS::SQS::Queue"]);
    }
  }

  async endHandler() {
    if (this.shouldExecute()) {
      this.log(`Ending!`);
      if (this.dynamoDbPoller) {
        this.dynamoDbPoller.stop();
      }
      if (this.sqsQueuePoller) {
        this.sqsQueuePoller.stop();
      }
    }
  }

  getResources() {
    return _.get(this.service, "resources", {});
  }

  async resourcesHandler(): Promise<StackResources> {
    let stackResources: StackResources = {
      "AWS::DynamoDB::Table": [],
      "AWS::SNS::Topic": [],
      "AWS::SQS::Queue": [],
    };

    const clients = this.clients();
    // const resources = this.resources;
    const stackName = `${this.service.service}-${this.stage}`;

    try {
      this.log(`[cloudformation][${stackName}] Creating stack.`);
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
      this.log(`[cloudformation][${stackName}] Stack created.`);
    } catch (createErr: any) {
      if ("name" in createErr && createErr.name !== "ValidationError") {
        this.warn(
          `[cloudformation] Unable to create stack - ${createErr.message}`
        );
        throw createErr;
      }

      try {
        this.log(
          `[cloudformation][${stackName}] Stack already exists. Updating stack.`
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

        this.log(`[cloudformation][${stackName}] Stack updated.`);
      } catch (updateErr: any) {
        this.warn(
          `[cloudformation] Unable to update stack - ${updateErr.message}`
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

      (stackResourcesResponse.StackResourceSummaries || []).forEach((r) => {
        if (Object.keys(stackResources).includes(r.ResourceType)) {
          if (!r.PhysicalResourceId) {
            return;
          }
          stackResources[r.ResourceType as SupportedResources].push({
            key: r.LogicalResourceId,
            id: r.PhysicalResourceId,
          });
        }
      });
    } catch (err: any) {
      this.warn(
        `[cloudformation] Unable to list stack resources - ${err.message}`
      );
      throw err;
    }

    return stackResources;
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
      sqs: new SQSClient(options),
    };
  }

  async dynamoDbHandler(tables: StackResource[]) {
    await Promise.all(
      tables.map(async (table) => {
        await this.createDynamoDbStreamPoller(table.key, table.id);
      })
    );
  }

  getFunctionsWithStreamEvent(type: "dynamodb", key: string) {
    return this.service.getAllFunctions().reduce((acc, functionName) => {
      const functionObject = this.service.getFunction(functionName);
      // TODO: support tables created outside of the stack
      const event = functionObject.events.find(
        (event: {
          stream?: { type?: string; arn?: { [x: string]: string[] } };
        }) => {
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
          return false;
        }
      );

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
    }, [] as DynamoDbFunctionDefinition[]);
  }

  async createDynamoDbStreamPoller(
    tableKey: string,
    tableName: string
  ): Promise<void> {
    const functions = this.getFunctionsWithStreamEvent("dynamodb", tableKey);

    const clients = this.clients();
    const table = await clients.dynamodb
      .describeTable({
        TableName: tableName,
      })
      .promise();

    if (!table.Table) {
      return;
    }

    const streamArn = table.Table.LatestStreamArn;

    if (!streamArn) {
      return;
    }

    this.log(
      `[dynamodb][${tableName}] Streaming to functions:`,
      functions.map((f) => f.functionName)
    );

    this.dynamoDbPoller = new DynamoDBStreamPoller(
      clients.dynamodbstreams,
      tableName,
      streamArn,
      functions,
      this.warn.bind(this)
    );

    return this.dynamoDbPoller.start();
  }

  async emitStreamRecords(
    records: _Record[],
    functionName: string,
    streamArn: string
  ) {
    if (!records || !records.length) {
      return;
    }
    const client = new LambdaClient({
      region: "us-east-1",
      apiVersion: "2015-03-31",
      endpoint: "http://localhost:3002",
    });
    const event = new MappedDynamoDBStreamEvent(
      records,
      this.region,
      streamArn
    );
    try {
      client.send(
        new InvokeCommand({
          FunctionName: functionName,
          Payload: event.stringify(),
          InvocationType: "Event",
        })
      );
      // TODO: Slice on errors and other settings?
    } catch (err: any) {
      this.warn(`[lambda][${functionName}] Error invoking -- ${err.message}`);
    }
  }

  async sqsHandler(queues: StackResource[]) {
    await Promise.all(
      queues.map(async (queue) => {
        await this.createSqsPoller(queue.key, queue.id);
      })
    );
  }

  getFunctionsWithSqsEvent(key: string) {
    return this.service.getAllFunctions().reduce((acc, functionName) => {
      const functionObject = this.service.getFunction(functionName);
      // TODO: support queues created outside of the stack
      console.log("!!! events", JSON.stringify(functionObject.events, null, 2));
      const event = functionObject.events.find(
        (event: { sqs?: { arn?: { [x: string]: string[] } } }) => {
          if (
            event.sqs &&
            event.sqs.arn &&
            event.sqs.arn["Fn::GetAtt"] &&
            event.sqs.arn["Fn::GetAtt"][0] === key &&
            event.sqs.arn["Fn::GetAtt"][1] === "Arn"
          ) {
            return true;
          }
          return false;
        }
      );

      if (!event) {
        return acc;
      }

      acc.push({
        functionName: functionObject.name,
        batchSize: event.batchSize || 1,
        // TODO: Filters and others?
        recordHandler: this.emitQueueRecords.bind(this),
      });
      return acc;
    }, [] as SqsFunctionDefinition[]);
  }

  async createSqsPoller(queueKey: string, queueUrl: string): Promise<void> {
    const functions = this.getFunctionsWithSqsEvent(queueKey);

    const clients = this.clients();

    this.log(
      `[sqs][${convertUrlToQueueName(queueUrl)}] Emitting to functions:`,
      functions.map((f) => f.functionName)
    );

    this.sqsQueuePoller = new SqsQueuePoller(
      clients.sqs,
      this.region,
      queueUrl,
      functions,
      this.log.bind(this)
    );

    return this.sqsQueuePoller.start();
  }

  async emitQueueRecords(
    records: Message[],
    functionName: string,
    queueArn: string
  ): Promise<string[]> {
    if (!records || !records.length) {
      return [];
    }

    const client = new LambdaClient({
      region: "us-east-1",
      apiVersion: "2015-03-31",
      endpoint: "http://localhost:3002",
    });

    const event = new MappedSQSEvent(records, this.region, queueArn);
    try {
      await client.send(
        new InvokeCommand({
          FunctionName: functionName,
          Payload: event.stringify(),
          InvocationType: "Event",
        })
      );

      return records.reduce((acc, record) => {
        if (record.ReceiptHandle) {
          acc.push(record.ReceiptHandle);
        }
        return acc;
      }, [] as string[]);
    } catch (err: any) {
      this.warn(`[lambda][${functionName}] Error invoking -- ${err.message}`);
      // TODO: DLQ or Retries?
      return [];
    }
  }
}

module.exports = ServerlessOfflineResources;
