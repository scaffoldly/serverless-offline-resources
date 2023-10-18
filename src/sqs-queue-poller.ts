import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  Message,
} from "@aws-sdk/client-sqs";

import { SQSEvent, SQSMessageAttributes, SQSRecord } from "aws-lambda";

export type SqsFunctionDefinition = {
  functionName: string;
  batchSize: number;
  recordHandler: (
    records: Message[],
    functionName: string,
    queueArn: string
  ) => Promise<string[]>;
};

// const convertArnToQueueName = (arn: string): string => {
//   const [, , , , , queueName] = arn.split(":");
//   return queueName;
// };

// const convertArnToUrl = (arn: string): string => {
//   const [, , , , accountId, queueName] = arn.split(":");
//   return `${LOCALSTACK_ENDPOINT}/${accountId}/${queueName}`;
// };

export const convertUrlToQueueName = (url: string): string => {
  const [, , , , queueName] = url.split("/");
  return queueName;
};

export const convertUrlToArn = (url: string, region: string): string => {
  const [, , , accountId, queueName] = url.split("/");
  // TODO: Infer region + localstack support
  return `arn:aws:sqs:${region}:${accountId}:${queueName}`;
};

export class SqsQueuePoller {
  queueName: string;
  queueArn: string;
  timeoutIds: Map<string, NodeJS.Timeout> = new Map();
  constructor(
    private client: SQSClient,
    region: string,
    private queueUrl: string,
    private functions: SqsFunctionDefinition[],
    private warn: (message: string, obj?: any) => void
  ) {
    this.queueName = convertUrlToQueueName(queueUrl);
    this.queueArn = convertUrlToArn(queueUrl, region);
  }

  // Doesn't really need to be async but its consistent with other poller start() methods
  async start() {
    this.functions.map(async (functionDefinition) => {
      this.timeoutIds.set(
        functionDefinition.functionName,
        setTimeout(() => this.getRecords(functionDefinition), 1000)
      );
    });
  }

  async getRecords(functionDefinition: SqsFunctionDefinition) {
    try {
      console.log(
        `!!! Getting records for ${functionDefinition.functionName} from ${this.queueUrl}`
      );

      // The error from vendia
      // Waiting on startup

      const result = await this.client.send(
        new ReceiveMessageCommand({
          QueueUrl: this.queueUrl,
          MaxNumberOfMessages: functionDefinition.batchSize,
          WaitTimeSeconds: 30, // TODO from function timeout / check AWS docs for defaults
        })
      );

      console.log(
        "!!! Received messages: ",
        JSON.stringify(result.Messages, null, 2)
      );

      if (result.Messages && result.Messages.length > 0) {
        let receiptHandles = await functionDefinition.recordHandler(
          result.Messages,
          functionDefinition.functionName,
          this.queueArn
        );

        await Promise.all(
          receiptHandles.map(async (receiptHandle) => {
            await this.client.send(
              new DeleteMessageCommand({
                QueueUrl: this.queueUrl,
                ReceiptHandle: receiptHandle,
              })
            );
          })
        );
      }
    } catch (e: any) {
      this.warn(
        `[${functionDefinition.functionName}][sqs][${this.queueName}] Unable to emit records.`,
        e
      );
    }

    this.timeoutIds.set(
      functionDefinition.functionName,
      setTimeout(() => this.getRecords(functionDefinition), 1000)
    );
  }

  stop() {
    this.timeoutIds.forEach((timeoutId) => clearTimeout(timeoutId));
  }
}

export class MappedSQSEvent implements SQSEvent {
  Records: SQSRecord[];
  textDecoder = new TextDecoder();

  constructor(messages: Message[], region: string, arn: string) {
    this.Records = messages.reduce((acc, record) => {
      const {
        MessageId: messageId,
        ReceiptHandle: receiptHandle,
        Body: body,
        Attributes: attributes,
        MessageAttributes: messageAttributesInternal,
        MD5OfBody: md5OfBody,
      } = record;

      if (
        !messageId ||
        !receiptHandle ||
        !body ||
        !attributes ||
        !messageAttributesInternal ||
        !md5OfBody
      ) {
        return acc;
      }

      const messageAttributes = Object.entries(
        messageAttributesInternal
      ).reduce(
        (
          acc,
          [
            key,
            {
              StringValue: stringValue,
              BinaryValue: binaryValueUInt8Array,
              DataType: dataType,
            },
          ]
        ) => {
          if (!dataType) {
            return acc;
          }

          const binaryValue = binaryValueUInt8Array
            ? this.textDecoder.decode(binaryValueUInt8Array)
            : undefined;

          acc[key] = {
            stringValue,
            binaryValue,
            dataType,
          };
          return acc;
        },
        {} as SQSMessageAttributes
      );

      acc.push({
        messageId,
        receiptHandle,
        body,
        attributes,
        messageAttributes,
        md5OfBody,
        eventSource: "aws:sqs",
        eventSourceARN: arn,
        awsRegion: region,
      });
      return acc;
    }, [] as SQSRecord[]);
  }

  stringify = (): string => {
    console.log("!!! Records: ", JSON.stringify({ Records: this.Records }));
    return JSON.stringify({ Records: this.Records });
  };
}
