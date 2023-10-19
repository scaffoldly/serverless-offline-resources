import { SQSClient, Message, PurgeQueueCommand } from "@aws-sdk/client-sqs";
import { SNSClient, SubscribeCommand } from "@aws-sdk/client-sns";

import { SNSEvent, SNSEventRecord } from "aws-lambda";
import { SqsQueuePoller } from "./sqs-queue-poller";

export type SnsFunctionDefinition = {
  functionName: string;
  recordHandler: (
    event: MappedSNSEvent,
    functionName: string,
    topicArn: string
  ) => Promise<void>;
};

export const convertArnToTopicName = (arn: string): string => {
  const [, , , , , topicName] = arn.split(":");
  return topicName;
};

export const convertArnToAccountId = (arn: string): string => {
  const [, , , , accountId] = arn.split(":");
  return accountId;
};

export class SnsPoller {
  topicName: string;
  sqsQueuePoller: SqsQueuePoller;
  subscriptionArn?: string;
  constructor(
    private snsClient: SNSClient,
    private sqsClient: SQSClient,
    region: string,
    private topicArn: string,
    private queueUrl: string,
    private functions: SnsFunctionDefinition[],
    private warn: (message: string, obj?: any) => void
  ) {
    this.topicName = convertArnToTopicName(topicArn);
    // We can't poll SNS directly, so we'll use SQS as the back channel
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

    // Subscribe
    // TODO Is this idempotent?
    const subscription = await this.snsClient.send(
      new SubscribeCommand({
        TopicArn: this.topicArn,
        Protocol: "sqs",
        Endpoint: this.sqsQueuePoller.queueArn,
        Attributes: {
          RawMessageDelivery: "false",
        },
      })
    );

    const { SubscriptionArn } = subscription;

    if (!SubscriptionArn) {
      this.warn(`[sns][${this.topicName}] Unable to subscribe to topic`);
      return;
    }

    this.subscriptionArn = SubscriptionArn;

    await this.sqsQueuePoller.start();
  }

  stop() {
    this.sqsQueuePoller.stop();
  }

  async emitQueueRecords(
    records: Message[],
    functionName: string
  ): Promise<string[]> {
    if (!records || !records.length) {
      return [];
    }

    if (!this.subscriptionArn) {
      return [];
    }

    const event = new MappedSNSEvent(records, this.subscriptionArn);
    if (!event.hasRecords()) {
      return [];
    }

    const functionDefinitions = this.functions.filter((fn) => {
      fn.functionName === functionName;
    });

    const receiptHandles = (
      await Promise.all(
        functionDefinitions.map(async (fn) => {
          try {
            await fn.recordHandler(event, fn.functionName, this.topicArn);
            return records.reduce((acc, record) => {
              if (record.ReceiptHandle) acc.push(record.ReceiptHandle);
              return acc;
            }, [] as string[]);
          } catch (err: any) {
            this.warn(`[sns][${this.topicName}] Error emitting records`, err);
            return [];
          }
        })
      )
    ).flat();

    return receiptHandles;
  }
}

export class MappedSNSEvent implements SNSEvent {
  Records: SNSEventRecord[];
  textDecoder = new TextDecoder();

  constructor(messages: Message[], subscriptionArn: string) {
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

      acc.push({
        EventVersion: "1.0",
        EventSubscriptionArn: subscriptionArn,
        EventSource: "aws:sns",
        Sns: JSON.parse(body),
      });
      return acc;
    }, [] as SNSEventRecord[]);
  }

  stringify = (): string => {
    return JSON.stringify({ Records: this.Records });
  };

  hasRecords = (): boolean => {
    return this.Records.length > 0;
  };
}
