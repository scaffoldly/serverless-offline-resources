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
import { SQSClient, GetQueueUrlCommand } from "@aws-sdk/client-sqs";
import { SNSClient } from "@aws-sdk/client-sns";
import {
  MappedSNSEvent,
  SnsFunctionDefinition,
  SnsPoller,
  convertArnToTopicName,
} from "./sns-poller";

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

type Resources = {
  Resources: {
    [key: string]: {
      Type: SupportedResources;
      Properties: any;
    };
  };
};

type ServerlessService = {
  service: string;
  custom?: ServerlessCustom;
  provider: {
    stage: string;
  };
  resources: Resources;
  getAllFunctions: () => string[];
  getFunction: (functionName: string) => {
    name: string;
    events?: {
      stream?: {
        type?: string;
        batchSize?: number;
        maximumRecordAgeInSeconds: number;
        arn?: { [x: string]: string[] };
      };
      sqs?: {
        batchSize?: number;
        maximumBatchingWindow?: number;
        arn?: { [x: string]: string[] };
      };
      sns?: {
        arn?: string;
        topicName?: string;
      };
    }[];
  };
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
  snsPoller?: SnsPoller;

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
    const resources = _.get(this.service, "resources");
    if (
      !resources ||
      !resources.Resources ||
      !Object.entries(resources).length
    ) {
      return resources;
    }

    const { Resources } = resources;

    Object.entries(Resources).reduce((acc, [key, value]) => {
      if (value.Type === "AWS::SNS::Topic") {
        // Inject a Queue to Bridge SNS to SQS
        acc[`${key}Queue`] = {
          Type: "AWS::SQS::Queue",
          Properties: {
            QueueName: `__${key}SNSBridge__`,
          },
        };
      }
      return acc;
    }, Resources);

    return resources;
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
      sns: new SNSClient(options),
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
      const { events } = functionObject;
      if (!events) {
        return acc;
      }

      events.forEach(({ stream }) => {
        if (
          stream &&
          stream.type === type &&
          stream.arn &&
          stream.arn["Fn::GetAtt"] &&
          stream.arn["Fn::GetAtt"][0] === key &&
          stream.arn["Fn::GetAtt"][1] === "StreamArn"
        ) {
          acc.push({
            functionName: functionObject.name,
            // TODO Slice on error and other properties
            batchSize: stream.batchSize || 1,
            maximumRecordAgeInSeconds:
              stream.maximumRecordAgeInSeconds || undefined,
            recordStreamHandler: this.emitStreamRecords.bind(this),
          });
        }
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
  ): Promise<void> {
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

    if (!event.hasRecords()) {
      return;
    }

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
      const { events } = functionObject;
      if (!events) {
        return acc;
      }

      events.forEach(({ sqs }) => {
        if (
          sqs &&
          sqs.arn &&
          sqs.arn["Fn::GetAtt"] &&
          sqs.arn["Fn::GetAtt"][0] === key &&
          sqs.arn["Fn::GetAtt"][1] === "Arn"
        ) {
          acc.push({
            functionName: functionObject.name,
            batchSize: sqs.batchSize || 10,
            waitTime: sqs.maximumBatchingWindow || 0,
            // TODO: Filters
            recordHandler: this.emitQueueRecords.bind(this),
          });
        }
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
      this.warn.bind(this)
    );

    return this.sqsQueuePoller.start();
  }

  async emitSnsEvent(
    event: MappedSNSEvent,
    functionName: string
  ): Promise<void> {
    if (!event || !event.Records || !event.Records.length) {
      return;
    }

    const client = new LambdaClient({
      region: "us-east-1",
      apiVersion: "2015-03-31",
      endpoint: "http://localhost:3002",
    });

    try {
      await client.send(
        new InvokeCommand({
          FunctionName: functionName,
          Payload: event.stringify(),
          InvocationType: "Event",
        })
      );
    } catch (err: any) {
      this.warn(`[lambda][${functionName}] Error invoking -- ${err.message}`);
      // TODO: DLQ or Retries?
      return;
    }
  }

  async snsHandler(topics: StackResource[]) {
    await Promise.all(
      topics.map(async (topic) => {
        await this.createSnsPoller(topic.key, topic.id);
      })
    );
  }

  getFunctionsWithSnsEvent(key: string) {
    return this.service.getAllFunctions().reduce((acc, functionName) => {
      const functionObject = this.service.getFunction(functionName);
      // TODO: support topics created outside of the stack
      const { events } = functionObject;
      if (!events) {
        return acc;
      }

      events.forEach(({ sns }) => {
        if (
          sns &&
          sns.arn &&
          sns.arn.split(" ").length == 2 &&
          sns.arn.split(" ")[0].trim() === `!Ref` &&
          sns.arn.split(" ")[1].trim() === key
        ) {
          acc.push({
            functionName: functionObject.name,
            // TODO: Filters
            recordHandler: this.emitSnsEvent.bind(this),
          });
        }
      });

      return acc;
    }, [] as SnsFunctionDefinition[]);
  }

  async createSnsPoller(topicKey: string, topicArn: string): Promise<void> {
    const functions = this.getFunctionsWithSnsEvent(topicKey);

    const clients = this.clients();

    this.log(
      `[sns][${convertArnToTopicName(topicArn)}] Emitting to functions:`,
      functions.map((f) => f.functionName)
    );

    const queue = await clients.sqs.send(
      new GetQueueUrlCommand({
        QueueName: `__${topicKey}SNSBridge__`,
      })
    );

    if (!queue.QueueUrl) {
      return;
    }

    this.snsPoller = new SnsPoller(
      clients.sns,
      clients.sqs,
      this.region,
      topicArn,
      queue.QueueUrl,
      functions,
      this.warn.bind(this)
    );

    return this.snsPoller.start();
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
    if (!event.hasRecords()) {
      return [];
    }

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
