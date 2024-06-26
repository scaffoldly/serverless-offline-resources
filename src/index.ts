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
import {
  EventBridgeFunctionDefinition,
  EventBridgePoller,
  MappedEventBridgeEvent,
} from "./eventbridge-poller";
import { retry } from "ts-retry-promise";
import { S3Client, Event as BucketEvent } from "@aws-sdk/client-s3";
import { MappedS3Event, S3FunctionDefinition, S3Poller } from "./s3-poller";
import { capitalizeKeys } from "./util";

export const LOCALSTACK_ENDPOINT =
  process.env.LOCALSTACK_ENDPOINT || "http://127.0.0.1:4566";
export const MOTO_ENDPOINT =
  process.env.MOTO_ENDPOINT || "http://127.0.0.1:5000";

const PLUGIN_NAME = "offline-resources";

type EventDrivenResources =
  | "AWS::DynamoDB::Table"
  | "AWS::SNS::Topic"
  | "AWS::SQS::Queue"
  | "AWS::Events::Rule"
  | "AWS::S3::Bucket";

// Generic Reources are used only for updating environment variables using their metadata
type GenericResources = "AWS::SecretsManager::Secret" | "AWS::KMS::Key";
const GENERIC_RESOURCES: GenericResources[] = [
  "AWS::SecretsManager::Secret",
  "AWS::KMS::Key",
];

type SupportedResources = EventDrivenResources | GenericResources;

type StackResources = { [key in SupportedResources]: StackResource[] };

type OfflineResourcesProps = {
  region?: string;
  cloudformation?: boolean | string[];
  uniqueId?: string;
  poll?: {
    dynamodb?: boolean | string[];
    eventbridge?: boolean | string[];
    sqs?: boolean | string[];
    sns?: boolean | string[];
    s3?: boolean | string[];
  };
};

type StackResource = {
  key: string;
  id: string;
  attributes: { [key: string]: string };
};

type ServerlessCustom = {
  "offline-resources"?: OfflineResourcesProps;
};

interface GenericResource {
  Type: GenericResources;
  Properties: {
    Name?: string;
  };
}

interface DynamoDBResource {
  Type: "AWS::DynamoDB::Table";
  Properties: {
    TableName: string;
  };
}

interface SNSResource {
  Type: "AWS::SNS::Topic";
  Properties: {
    TopicName: string;
  };
}

interface SQSResource {
  Type: "AWS::SQS::Queue";
  Properties: {
    QueueName: string;
  };
}

interface S3Resource {
  Type: "AWS::S3::Bucket";
  Properties: { [key: string]: any };
}

interface EventBridgeResource {
  Type: "AWS::Events::Rule";
  Properties: {
    Name: string;
    State: "ENABLED";
    ScheduleExpression?: string;
    // TODO: inputs
    Targets: {
      Id: string;
      Arn: { "Fn::GetAtt": string[] };
      Input?: string;
    }[];
  };
}

type Resources = {
  Resources: {
    [key: string]:
      | GenericResource
      | DynamoDBResource
      | SNSResource
      | SQSResource
      | S3Resource
      | EventBridgeResource;
  };
};

type ServerlessService = {
  service: string;
  custom?: ServerlessCustom;
  provider: {
    stage: string;
    environment?: {
      [key: string]:
        | string
        | { Ref?: string }
        | {
            "Fn::GetAtt"?: [string, string];
          };
    };
    s3?: {
      [key: string]: {
        name?: string;
        // All other props will be converted to PascalCase
      };
    };
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
        arn?: { Ref?: string }; // TODO: support hardcoded
        topicName?: string;
      };
      eventBridge?: {
        name?: string;
        schedule?: string;
        input?: { [key: string]: string };
      };
      s3?: {
        bucket?: string | { Ref?: string };
        event?: BucketEvent;
        // TODO: Support Rules
        // TODO: Create the bucket when this is false
        existing?: boolean;
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
  started: boolean;
  service: ServerlessService;
  config: OfflineResourcesProps;
  provider: "aws";
  hooks: {
    "before:offline:start": () => void;
    "before:offline:start:init": () => void;
    "before:offline:start:end": () => void;
  };

  dynamoDbPoller?: DynamoDBStreamPoller;
  sqsQueuePoller?: SqsQueuePoller;
  snsPoller?: SnsPoller;
  eventBridgePoller?: EventBridgePoller;
  s3Poller?: S3Poller;

  serviceName: string;

  constructor(serverless: Serverless, private options: Options) {
    this.started = false;

    this.service = serverless.service;
    this.serviceName = this.service.service;
    this.config =
      (this.service.custom && this.service.custom[PLUGIN_NAME]) || {};

    this.options = options;
    this.provider = "aws";

    this.hooks = {
      "before:offline:start": this.startHandler.bind(this),
      "before:offline:start:init": this.startHandler.bind(this),
      "before:offline:start:end": this.endHandler.bind(this),
    };
  }

  log(message: string, obj?: any) {
    msg(console.log, this.stage, message, obj);
  }

  warn(message: string, obj?: any) {
    msg(console.warn, this.stage, message, obj);
  }

  get region() {
    const config =
      (this.service.custom && this.service.custom[PLUGIN_NAME]) || {};
    return _.get(config, "region", "us-east-1");
  }

  get stage() {
    return (
      (this.options && this.options.stage) ||
      (this.service.provider && this.service.provider.stage)
    );
  }

  async startHandler() {
    if (this.started) {
      return;
    }
    this.started = true;

    this.log(`Starting...`);

    let resources: StackResources = {
      "AWS::SecretsManager::Secret": [],
      "AWS::DynamoDB::Table": [],
      "AWS::SNS::Topic": [],
      "AWS::SQS::Queue": [],
      "AWS::Events::Rule": [],
      "AWS::S3::Bucket": [],
      "AWS::KMS::Key": [],
    };

    if (
      this.config.cloudformation === undefined ||
      this.config.cloudformation === true ||
      (Array.isArray(this.config.cloudformation) &&
        this.config.cloudformation.includes(this.stage))
    ) {
      resources = await retry(async () => await this.cloudformationHandler(), {
        backoff: "EXPONENTIAL",
        delay: 1000,
        maxBackOff: 10000,
        retries: 5,
      });
    } else {
      // TODO: Enrich things with IDs if not using cloudformation
    }

    await this.updateEnvironment(resources);

    if (
      this.config.poll === undefined ||
      this.config.poll.dynamodb === undefined ||
      this.config.poll.dynamodb === true ||
      (Array.isArray(this.config.poll.dynamodb) &&
        this.config.poll.dynamodb.includes(this.stage))
    ) {
      await this.dynamoDbHandler(resources["AWS::DynamoDB::Table"]);
    }

    if (
      this.config.poll === undefined ||
      this.config.poll.eventbridge === undefined ||
      this.config.poll.eventbridge === true ||
      (Array.isArray(this.config.poll.eventbridge) &&
        this.config.poll.eventbridge.includes(this.stage))
    ) {
      await this.eventBridgeHandler(resources["AWS::Events::Rule"]);
    }

    if (
      this.config.poll === undefined ||
      this.config.poll.sqs === undefined ||
      this.config.poll.sqs === true ||
      (Array.isArray(this.config.poll.sqs) &&
        this.config.poll.sqs.includes(this.stage))
    ) {
      await this.sqsHandler(resources["AWS::SQS::Queue"]);
    }

    if (
      this.config.poll === undefined ||
      this.config.poll.sns === undefined ||
      this.config.poll.sns === true ||
      (Array.isArray(this.config.poll.sns) &&
        this.config.poll.sns.includes(this.stage))
    ) {
      await this.snsHandler(resources["AWS::SNS::Topic"]);
    }

    if (
      this.config.poll === undefined ||
      this.config.poll.s3 === undefined ||
      this.config.poll.s3 === true ||
      (Array.isArray(this.config.poll.s3) &&
        this.config.poll.s3.includes(this.stage))
    ) {
      await this.s3Handler(resources["AWS::S3::Bucket"]);
    }
  }

  uniqueify(name: string, separator = "-") {
    return `${
      this.config.uniqueId ? `${this.config.uniqueId}${separator}` : ""
    }${name}`;
  }

  async endHandler() {
    this.log(`Ending!`);
    if (this.dynamoDbPoller) {
      this.dynamoDbPoller.stop();
    }
    if (this.sqsQueuePoller) {
      this.sqsQueuePoller.stop();
    }
    if (this.snsPoller) {
      this.snsPoller.stop();
    }
    if (this.s3Poller) {
      this.s3Poller.stop();
    }
    if (this.eventBridgePoller) {
      this.eventBridgePoller.stop();
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
      if (
        value.Properties &&
        GENERIC_RESOURCES.includes(value.Type as GenericResources)
      ) {
        value = value as GenericResource;
        if (value.Properties.Name) {
          value.Properties.Name = this.uniqueify(value.Properties.Name);
        }
      }

      if (
        value.Properties &&
        value.Type === "AWS::DynamoDB::Table" &&
        value.Properties.TableName
      ) {
        value.Properties.TableName = this.uniqueify(value.Properties.TableName);
      }

      if (
        value.Properties &&
        value.Type === "AWS::SQS::Queue" &&
        value.Properties.QueueName
      ) {
        value.Properties.QueueName = this.uniqueify(value.Properties.QueueName);
      }

      if (
        value.Properties &&
        value.Type === "AWS::SNS::Topic" &&
        value.Properties.TopicName
      ) {
        value.Properties.TopicName = this.uniqueify(value.Properties.TopicName);
      }

      if (value.Type === "AWS::SNS::Topic") {
        // Inject a Queue to Bridge SNS to SQS
        acc[`${key}Queue`] = {
          Type: "AWS::SQS::Queue",
          Properties: {
            QueueName: `__${this.uniqueify(key, "_")}SNSBridge__`,
          },
        };
      }

      // This is for supporting S3 events where "existing: true"
      if (value.Type === "AWS::S3::Bucket") {
        // Inject a Queue to Bridge S3 Events to SQS
        acc[`${key}Queue`] = {
          Type: "AWS::SQS::Queue",
          Properties: {
            QueueName: `__${this.uniqueify(key, "_")}S3Bridge__`,
          },
        };
      }

      acc[key] = value;

      return acc;
    }, Resources);

    // TODO: add provider.s3 support, which will internally create an AWS::S3::Bucket in the resources list
    // TODO: for all functions with s3 events and existing being false, create an AWS::S3::Bucket in the resources list

    this.getFunctionsWithEventBridgeEvent().forEach((fn) => {
      const queueKey = `${fn.ruleName}Queue`;

      // Create a queue as the backhaul
      Resources[queueKey] = {
        Type: "AWS::SQS::Queue",
        Properties: {
          QueueName: `__${this.uniqueify(fn.ruleName, "_")}EventBridge__`,
        },
      };

      // Set up a rule to emit to the queue
      Resources[fn.ruleName] = {
        Type: "AWS::Events::Rule",
        Properties: {
          Name: fn.ruleName,
          State: "ENABLED",
          ScheduleExpression: fn.schedule,
          // TODO Patterns
          Targets: [
            {
              Id: `${fn.ruleName}-target`,
              Arn: {
                "Fn::GetAtt": [queueKey, "Arn"],
              },
              Input: fn.input ? JSON.stringify(fn.input) : undefined,
            },
          ],
        },
      };
    });

    // This is for supporting SNS events where "existing: false"
    this.getFunctionsWithS3Event().forEach((fn) => {
      if (fn.existing || !fn.providerS3Key) {
        // "existing" buckets are wired up above
        return;
      }

      const queueKey = `${fn.bucketKey}Queue`;

      // Create a queue as the backhaul
      Resources[queueKey] = {
        Type: "AWS::SQS::Queue",
        Properties: {
          QueueName: `__${this.uniqueify(fn.bucketKey, "_")}S3Bridge__`,
        },
      };

      const bucketDefinition = (this.service.provider.s3 || {})[
        fn.providerS3Key
      ];

      // TODO: Support buckets that are defined at the function level

      if (!bucketDefinition) {
        this.log(
          `[s3] No bucket definition of \`${fn.providerS3Key}\` found in \`provider.s3\`.`
        );
        return;
      }

      // Create a bucket as defined in "provider.s3"
      Resources[fn.bucketKey] = {
        Type: "AWS::S3::Bucket",
        Properties: {
          BucketName: bucketDefinition.name,
          ...capitalizeKeys(bucketDefinition),
          Name: undefined,
        },
      };
    });

    return resources;
  }

  async updateEnvironment(stackResources: StackResources) {
    Object.values(stackResources).forEach((stackResource) => {
      Object.values(stackResource).forEach((resource) => {
        this.service.provider.environment = Object.entries(
          this.service.provider.environment || {}
        ).reduce((acc, [key, value]) => {
          if (
            typeof value !== "string" &&
            "Ref" in value &&
            value.Ref &&
            value.Ref === resource.key
          ) {
            acc[key] = resource.id;
          }

          if (
            typeof value !== "string" &&
            "Fn::GetAtt" in value &&
            value["Fn::GetAtt"] &&
            value["Fn::GetAtt"][0] === resource.key &&
            typeof value["Fn::GetAtt"][1] === "string"
          ) {
            acc[key] = resource.attributes[value["Fn::GetAtt"][1]];
          }

          return acc;
        }, this.service.provider.environment || {});
      });
    });
  }

  async cloudformationHandler(): Promise<StackResources> {
    let stackResources: StackResources = {
      "AWS::SecretsManager::Secret": [],
      "AWS::DynamoDB::Table": [],
      "AWS::SNS::Topic": [],
      "AWS::SQS::Queue": [],
      "AWS::Events::Rule": [],
      "AWS::S3::Bucket": [],
      "AWS::KMS::Key": [],
    };

    const clients = this.clients();
    // const resources = this.resources;
    const stackName = this.uniqueify(`${this.service.service}-${this.stage}`);

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
      if (
        "name" in createErr &&
        createErr.name !== "ValidationError" &&
        createErr.name !== "AlreadyExistsException"
      ) {
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
        if ("name" in updateErr && updateErr.name !== "ValidationError") {
          this.warn(
            `[cloudformation] Unable to update stack - ${updateErr.message}`
          );
          throw createErr;
        }
      }
    }

    try {
      const stackResourcesResponse = await clients.cloudformation
        .listStackResources({
          StackName: stackName,
        })
        .promise();

      return (stackResourcesResponse.StackResourceSummaries || []).reduce(
        async (accP, r) => {
          const acc = await accP;
          if (Object.keys(acc).includes(r.ResourceType)) {
            if (!r.PhysicalResourceId) {
              return acc;
            }
            acc[r.ResourceType as SupportedResources].push({
              key: r.LogicalResourceId,
              id: r.PhysicalResourceId,
              attributes: await this.attributes(
                r.ResourceType as SupportedResources,
                r.PhysicalResourceId
              ),
            });
          }
          return acc;
        },
        Promise.resolve(stackResources)
      );
    } catch (err: any) {
      this.warn(
        `[cloudformation] Unable to list stack resources - ${err.message}`
      );
      throw err;
    }
  }

  async attributes(
    type: SupportedResources,
    id: string
  ): Promise<{ [key: string]: string }> {
    if (type === "AWS::S3::Bucket") {
      // TODO: Support non-localstack/moto resources
      const url = new URL(LOCALSTACK_ENDPOINT);
      return {
        DomainName: `${id}.s3.${url.host}`,
      };
    }
    return {};
  }

  clients() {
    let endpoint: string | undefined = undefined;
    let credentials: AWS.Credentials | undefined = undefined;
    let forcePathStyle = false;

    if (process.env.LOCALSTACK === "true") {
      endpoint = LOCALSTACK_ENDPOINT;
      credentials = new AWS.Credentials("test", "test");
      forcePathStyle = true;
    }

    if (process.env.MOTO === "true") {
      endpoint = MOTO_ENDPOINT;
      credentials = new AWS.Credentials("testing", "testing");
      forcePathStyle = true;
    }

    let options = {
      region: this.region,
      endpoint,
      credentials,
    };

    return {
      cloudformation: new AWS.CloudFormation(options),
      dynamodb: new AWS.DynamoDB(options),
      dynamodbstreams: new DynamoDBStreamsClient(options),
      sns: new SNSClient(options),
      sqs: new SQSClient(options),
      s3: new S3Client({ ...options, forcePathStyle }),
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

    if (!functions.length) {
      return;
    }

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
      await client.send(
        new InvokeCommand({
          FunctionName: functionName,
          Payload: event.stringify(),
          InvocationType: "Event", // TODO: Perhaps RequestResponse for error handling
        })
      );
      // TODO: Slice on errors and other settings?
    } catch (err: any) {
      this.warn(`[lambda][${functionName}] Error invoking`, err);
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

    if (!functions.length) {
      return;
    }

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
          InvocationType: "Event", // TODO: Perhaps RequestResponse for error handling
        })
      );

      return records.reduce((acc, record) => {
        if (record.ReceiptHandle) {
          acc.push(record.ReceiptHandle);
        }
        return acc;
      }, [] as string[]);
    } catch (err: any) {
      this.warn(`[lambda][${functionName}] Error invoking`, err);
      // TODO: DLQ or Retries?
      return [];
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
        if (sns && sns.arn && sns.arn.Ref && sns.arn.Ref === key) {
          acc.push({
            functionName: functionObject.name,
            // TODO: Support TopicName
            recordHandler: this.emitSnsEvent.bind(this),
          });
        }
      });

      return acc;
    }, [] as SnsFunctionDefinition[]);
  }

  async createSnsPoller(topicKey: string, topicArn: string): Promise<void> {
    const functions = this.getFunctionsWithSnsEvent(topicKey);

    if (!functions.length) {
      return;
    }

    const clients = this.clients();

    this.log(
      `[sns][${convertArnToTopicName(topicArn)}] Emitting to functions:`,
      functions.map((f) => f.functionName)
    );

    const queue = await clients.sqs.send(
      new GetQueueUrlCommand({
        QueueName: `__${this.uniqueify(topicKey, "_")}SNSBridge__`,
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
          InvocationType: "Event", // TODO: Perhaps RequestResponse for error handling
        })
      );
    } catch (err: any) {
      this.warn(`[lambda][${functionName}] Error invoking`, err);
      // TODO: DLQ or Retries?
      return;
    }
  }

  async s3Handler(buckets: StackResource[]) {
    await Promise.all(
      buckets.map(async (bucket) => {
        await this.createS3Poller(bucket.key, bucket.id);
      })
    );
  }

  getFunctionsWithS3Event(key?: string) {
    return this.service.getAllFunctions().reduce((acc, functionName) => {
      const functionObject = this.service.getFunction(functionName);
      // TODO: support topics created outside of the stack
      const { events } = functionObject;
      if (!events) {
        return acc;
      }

      events.forEach(({ s3 }) => {
        if (
          s3 &&
          s3.bucket &&
          typeof s3.bucket === "string" &&
          !s3.existing &&
          (!key || key === `S3Bucket${s3.bucket}`)
        ) {
          acc.push({
            functionName: functionObject.name,
            bucketKey: `S3Bucket${s3.bucket}`,
            providerS3Key: s3.bucket,
            event: s3.event || "s3:ObjectCreated:*",
            recordHandler: this.emitS3Event.bind(this),
            existing: false,
          });
        }

        if (
          s3 &&
          s3.bucket &&
          s3.existing === true &&
          typeof s3.bucket !== "string" &&
          s3.bucket.Ref &&
          s3.bucket.Ref === key
        ) {
          acc.push({
            functionName: functionObject.name,
            bucketKey: key,
            event: s3.event || "s3:ObjectCreated:*",
            recordHandler: this.emitS3Event.bind(this),
            existing: true,
          });
        }
      });

      return acc;
    }, [] as S3FunctionDefinition[]);
  }

  async createS3Poller(key: string, bucketName: string): Promise<void> {
    const functions = this.getFunctionsWithS3Event(key);

    if (!functions.length) {
      return;
    }

    const clients = this.clients();

    this.log(
      `[s3][${bucketName}] Emitting to functions:`,
      functions.map((f) => f.functionName)
    );

    const queue = await clients.sqs.send(
      new GetQueueUrlCommand({
        QueueName: `__${this.uniqueify(key, "_")}S3Bridge__`,
      })
    );

    if (!queue.QueueUrl) {
      return;
    }

    this.s3Poller = new S3Poller(
      clients.s3,
      clients.sqs,
      this.region,
      bucketName,
      queue.QueueUrl,
      functions,
      this.warn.bind(this)
    );

    return this.s3Poller.start();
  }

  async emitS3Event(event: MappedS3Event, functionName: string): Promise<void> {
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
          InvocationType: "Event", // TODO: Perhaps RequestResponse for error handling
        })
      );
    } catch (err: any) {
      this.warn(`[lambda][${functionName}] Error invoking`, err);
      // TODO: DLQ or Retries?
      return;
    }
  }

  async eventBridgeHandler(rules: StackResource[]) {
    await Promise.all(
      rules.map(async (rule) => {
        await this.createEventBridgePoller(rule.key);
      })
    );
  }

  getFunctionsWithEventBridgeEvent(ruleName?: string) {
    return this.service.getAllFunctions().reduce((acc, functionName) => {
      const functionObject = this.service.getFunction(functionName);
      // TODO: support topics created outside of the stack
      const { events } = functionObject;
      if (!events) {
        return acc;
      }

      const serviceName = this.service.service;
      const shortName = functionName.split("-").slice(-1)[0];
      const stage = this.stage;

      events.forEach(({ eventBridge }, ix) => {
        const generatedName = `${serviceName}-${shortName}-${stage}-rule-${
          ix + 1
        }`;
        if (
          (eventBridge && !ruleName) ||
          (eventBridge &&
            ruleName &&
            (eventBridge.name === ruleName || generatedName === ruleName))
        ) {
          acc.push({
            functionName: functionObject.name,
            // todo: make input work
            ruleName: eventBridge.name || generatedName,
            schedule: eventBridge.schedule,
            input: eventBridge.input,
            // TODO: Support TopicName
            recordHandler: this.emitEventBridgeEvent.bind(this),
          });
        }
      });

      return acc;
    }, [] as EventBridgeFunctionDefinition[]);
  }

  async createEventBridgePoller(ruleName: string): Promise<void> {
    const functions = this.getFunctionsWithEventBridgeEvent(ruleName);

    if (!functions.length) {
      return;
    }

    const clients = this.clients();

    this.log(
      `[eventbridge][${ruleName}] Emitting to functions:`,
      functions.map((f) => f.functionName)
    );

    const queue = await clients.sqs.send(
      new GetQueueUrlCommand({
        QueueName: `__${this.uniqueify(ruleName, "_")}EventBridge__`,
      })
    );

    if (!queue.QueueUrl) {
      return;
    }

    this.eventBridgePoller = new EventBridgePoller(
      clients.sqs,
      this.region,
      ruleName,
      queue.QueueUrl,
      functions,
      this.warn.bind(this)
    );

    return this.eventBridgePoller.start();
  }

  async emitEventBridgeEvent(
    event: MappedEventBridgeEvent,
    functionName: string
  ): Promise<void> {
    if (!event || !event.events || !event.events.length) {
      return;
    }

    const client = new LambdaClient({
      region: "us-east-1",
      apiVersion: "2015-03-31",
      endpoint: "http://localhost:3002",
    });

    await Promise.all(
      event.events.map(async (event) => {
        try {
          await client.send(
            new InvokeCommand({
              FunctionName: functionName,
              Payload: JSON.stringify(event),
              InvocationType: "Event", // TODO: Perhaps RequestResponse for error handling
            })
          );
        } catch (err: any) {
          this.warn(`[lambda][${functionName}] Error invoking`, err);
          // TODO: DLQ or Retries?
          return;
        }
      })
    );
  }
}

module.exports = ServerlessOfflineResources;
