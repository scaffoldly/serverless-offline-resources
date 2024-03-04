import { SQSClient, Message, PurgeQueueCommand } from "@aws-sdk/client-sqs";
import { EventBridgeEvent } from "aws-lambda";
import { SqsQueuePoller, convertUrlToArn } from "./sqs-queue-poller";

export type EventBridgeEventType = "Scheduled Event";

export type EventBridgeFunctionDefinition = {
  functionName: string;
  ruleName: string;
  schedule?: string;
  recordHandler: (
    event: MappedEventBridgeEvent,
    functionName: string,
    ruleArn: string
  ) => Promise<void>;
};
export const convertRuleNameToRuleArn = (
  ruleName: string,
  queueUrl: string,
  region: string
) => {
  const queueArn = convertUrlToArn(queueUrl, region);
  const [, , , , accountId] = queueArn.split(":");
  return `arn:aws:events:${region}:${accountId}:rule/${ruleName}`;
};

export const convertArnToRuleName = (arn: string): string => {
  const [, , , , name] = arn.split("/");
  return name;
};

export const convertArnToAccountId = (arn: string): string => {
  const [, , , , accountId] = arn.split(":");
  return accountId;
};

export const convertArnToRegion = (arn: string): string => {
  const [, , , region] = arn.split(":");
  return region;
};

export class EventBridgePoller {
  ruleArn: string;
  sqsQueuePoller: SqsQueuePoller;
  constructor(
    private sqsClient: SQSClient,
    region: string,
    private ruleName: string,
    private queueUrl: string,
    private functions: EventBridgeFunctionDefinition[],
    private warn: (message: string, obj?: any) => void
  ) {
    this.ruleArn = convertRuleNameToRuleArn(ruleName, queueUrl, region);
    // We can't poll Eventbridge directly, so we'll use SQS as the back channel
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

    const event = new MappedEventBridgeEvent(records, this.ruleArn);
    if (!event.hasRecords()) {
      return [];
    }

    const functionDefinitions = this.functions.filter(
      (fn) => fn.functionName === functionName
    );

    const receiptHandles = (
      await Promise.all(
        functionDefinitions.map(async (fn) => {
          try {
            await fn.recordHandler(event, fn.functionName, this.ruleArn);
            return records.reduce((acc, record) => {
              if (record.ReceiptHandle) acc.push(record.ReceiptHandle);
              return acc;
            }, [] as string[]);
          } catch (err: any) {
            this.warn(
              `[eventbridge][${this.ruleName}] Error emitting records`,
              err
            );
            return [];
          }
        })
      )
    ).flat();

    return receiptHandles;
  }
}

export class MappedEventBridgeEvent {
  events: EventBridgeEvent<string, unknown>[];

  constructor(messages: Message[], ruleArn: string) {
    this.events = messages.reduce((acc, record) => {
      const {
        MessageId: messageId,
        ReceiptHandle: receiptHandle,
        Body: body,
        MD5OfBody: md5OfBody,
      } = record;
      if (!messageId || !receiptHandle || !body || !md5OfBody) {
        return acc;
      }

      try {
        const detail = JSON.parse(body) as EventBridgeEvent<string, unknown>;

        if (detail.resources?.[0] !== ruleArn) {
          return acc;
        }

        acc.push(detail);
      } catch (e) {}

      return acc;
    }, [] as EventBridgeEvent<string, unknown>[]);
  }

  hasRecords = (): boolean => {
    return this.events.length > 0;
  };
}
