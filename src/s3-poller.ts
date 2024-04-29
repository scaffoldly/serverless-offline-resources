import { SQSClient, Message, PurgeQueueCommand } from "@aws-sdk/client-sqs";

import { S3Event, S3EventRecord } from "aws-lambda";
import { SqsQueuePoller } from "./sqs-queue-poller";
import {
  PutBucketNotificationConfigurationCommand,
  S3Client,
  Event as BucketEvent,
} from "@aws-sdk/client-s3";

export type S3FunctionDefinition = {
  functionName: string;
  key: string;
  existing: boolean;
  event: BucketEvent;
  recordHandler: (
    event: MappedS3Event,
    functionName: string,
    bucketName: string
  ) => Promise<void>;
};

type S3TestEvent = {
  Event: "s3:TestEvent";
};

export class S3Poller {
  sqsQueuePoller: SqsQueuePoller;
  constructor(
    private s3Client: S3Client,
    private sqsClient: SQSClient,
    region: string,
    private bucketName: string,
    private queueUrl: string,
    private functions: S3FunctionDefinition[],
    private warn: (message: string, obj?: any) => void
  ) {
    // We can't poll S3 directly, so we'll use SQS as the back channel
    this.sqsQueuePoller = new SqsQueuePoller(
      sqsClient,
      region,
      queueUrl,
      this.functions.map((fn) => ({
        functionName: fn.functionName,
        batchSize: 1,
        waitTime: 0,
        recordHandler: this.emitQueueRecords.bind(this),
      })),
      this.warn
    );
  }

  async start() {
    // Purge the queue
    await this.sqsClient.send(
      new PurgeQueueCommand({ QueueUrl: this.queueUrl })
    );

    // Create Bucket Notifications to SQS
    // TODO: is this idempotent
    const response = await this.s3Client.send(
      new PutBucketNotificationConfigurationCommand({
        Bucket: this.bucketName,
        NotificationConfiguration: {
          QueueConfigurations: this.functions.map((fn) => ({
            QueueArn: this.sqsQueuePoller.queueArn,
            Events: [fn.event],
          })),
        },
      })
    );

    if (response.$metadata.httpStatusCode !== 200) {
      this.warn(
        `[s3][${this.bucketName}] Unable to create bucket notification`
      );
      return;
    }

    await this.sqsQueuePoller.start();
  }

  stop() {
    this.sqsQueuePoller.stop();
  }

  async emitQueueRecords(
    records: Message[],
    functionName: string
  ): Promise<string[]> {
    let receiptHandles: string[] = [];

    if (!records || !records.length) {
      return receiptHandles;
    }

    const event = new MappedS3Event(records);

    if (event.TestMessageReceiptHandle) {
      receiptHandles.push(event.TestMessageReceiptHandle);
    }

    if (!event.hasRecords()) {
      return receiptHandles;
    }

    const functionDefinitions = this.functions.filter(
      (fn) => fn.functionName === functionName
    );

    receiptHandles = receiptHandles.concat(
      ...(
        await Promise.all(
          functionDefinitions.map(async (fn) => {
            try {
              await fn.recordHandler(event, fn.functionName, this.bucketName);
              return records.reduce((acc, record) => {
                if (record.ReceiptHandle) acc.push(record.ReceiptHandle);
                return acc;
              }, [] as string[]);
            } catch (err: any) {
              this.warn(
                `[sns][${this.bucketName}] Error emitting records`,
                err
              );
              return [];
            }
          })
        )
      ).flat()
    );

    return receiptHandles;
  }
}

export class MappedS3Event implements S3Event {
  Records: S3EventRecord[];
  TestMessageReceiptHandle: string | undefined = undefined;

  constructor(messages: Message[]) {
    this.Records = messages.reduce((acc, record) => {
      const {
        MessageId: messageId,
        ReceiptHandle: receiptHandle,
        Body: body,
        MD5OfBody: md5OfBody,
      } = record;

      if (!messageId || !receiptHandle || !body || !md5OfBody) {
        return acc;
      }

      const event = JSON.parse(body) as S3Event | S3TestEvent;
      if ("Records" in event && event.Records && event.Records.length) {
        acc = acc.concat(...event.Records);
        return acc;
      }

      if ("Event" in event && event.Event && event.Event === "s3:TestEvent") {
        this.TestMessageReceiptHandle = receiptHandle;
      }

      return acc;
    }, [] as S3EventRecord[]);
  }

  stringify = (): string => {
    return JSON.stringify({ Records: this.Records });
  };

  hasRecords = (): boolean => {
    return this.Records.length > 0;
  };
}
