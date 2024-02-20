import {
  DescribeStreamCommand,
  DynamoDBStreamsClient,
  GetRecordsCommand,
  GetShardIteratorCommand,
  _Record,
} from "@aws-sdk/client-dynamodb-streams";
import { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";
import { retry } from "ts-retry-promise";

export type DynamoDbFunctionDefinition = {
  functionName: string;
  batchSize: number;
  maximumRecordAgeInSeconds?: number;
  recordStreamHandler: (
    records: _Record[],
    functionName: string,
    streamArn: string
  ) => Promise<void>;
};

export class DynamoDBStreamPoller {
  shardIterators: Map<string, string>;
  timeoutIds: Map<string, NodeJS.Timeout>;
  recordQueues: Map<string, _Record[]>;
  constructor(
    private client: DynamoDBStreamsClient,
    private tableName: string,
    private streamArn: string,
    private functions: DynamoDbFunctionDefinition[],
    private warn: (message: string, obj?: any) => void
  ) {
    this.client = client;
    this.streamArn = streamArn;
    this.functions = functions;
    this.shardIterators = new Map();
    this.timeoutIds = new Map();
    this.recordQueues = new Map();
  }

  async start() {
    try {
      const { StreamDescription } = await this.client.send(
        new DescribeStreamCommand({ StreamArn: this.streamArn })
      );

      const Shards = (StreamDescription || {}).Shards || [];

      for (const shard of (StreamDescription || {}).Shards || []) {
        const { SequenceNumberRange, ShardId } = shard;
        if (!SequenceNumberRange || !ShardId) {
          continue;
        }
        const { ShardIterator } = await retry(() =>
          this.client.send(
            new GetShardIteratorCommand({
              ShardId: ShardId,
              ShardIteratorType: "LATEST",
              StreamArn: this.streamArn,
              SequenceNumber: SequenceNumberRange.StartingSequenceNumber,
            })
          )
        );

        if (!ShardIterator) continue;

        this.shardIterators.set(ShardId, ShardIterator);
        this.recordQueues.set(ShardId, []);
      }

      Shards.map(async ({ ShardId }) => {
        if (!ShardId) return;
        this.timeoutIds.set(
          ShardId,
          setTimeout(() => this.getRecords(ShardId), 1000)
        );
      });
    } catch (e: any) {
      this.warn(`[dynamodb][${this.tableName}] Unable to create streams`, e);
    }
  }

  async getRecords(shardId: string) {
    if (!shardId) return;

    try {
      const shardIterator = this.shardIterators.get(shardId);
      if (!shardIterator) return;

      const { Records, NextShardIterator } = await this.client.send(
        new GetRecordsCommand({ ShardIterator: shardIterator })
      );

      const recordQueue = this.recordQueues.get(shardId);
      if (!recordQueue) return;

      if (Records && Records.length > 0) {
        recordQueue.push(...Records);
      }

      try {
        await Promise.all(
          this.functions.map(async (functionConfig) => {
            let filteredRecords = recordQueue.splice(
              0,
              functionConfig.batchSize || recordQueue.length
            );

            const { maximumRecordAgeInSeconds } = functionConfig;

            if (
              maximumRecordAgeInSeconds !== null &&
              maximumRecordAgeInSeconds !== undefined
            ) {
              const now = Date.now();
              filteredRecords = filteredRecords.filter(({ dynamodb }) => {
                if (!dynamodb) {
                  return false;
                }
                const { ApproximateCreationDateTime } = dynamodb;
                if (!ApproximateCreationDateTime) {
                  return false;
                }
                const recordAgeInSeconds =
                  (now - ApproximateCreationDateTime.getTime()) / 1000;
                return recordAgeInSeconds <= maximumRecordAgeInSeconds;
              });
            }

            await functionConfig.recordStreamHandler(
              filteredRecords,
              functionConfig.functionName,
              this.streamArn
            );
          })
        );
      } catch (handlerError: any) {
        this.warn(
          `[dynamodb][${this.tableName}] Unable to handle records`,
          handlerError
        );
      }

      if (NextShardIterator) {
        this.shardIterators.set(shardId, NextShardIterator);
      }

      if (recordQueue.length > 0) {
        this.getRecords(shardId);
      }
    } catch (error: any) {
      this.warn(`[dynamodb][${this.tableName}] Unable to emit records`, error);
    }

    this.timeoutIds.set(
      shardId,
      setTimeout(() => this.getRecords(shardId), 1000)
    );
  }

  stop() {
    this.timeoutIds.forEach((timeoutId) => clearTimeout(timeoutId));
  }
}

export class MappedDynamoDBStreamEvent implements DynamoDBStreamEvent {
  Records: DynamoDBRecord[];

  constructor(
    public records: _Record[],
    public region: string,
    public streamArn: string
  ) {
    this.Records = records.reduce((acc, record) => {
      const {
        dynamodb,
        eventID,
        eventName,
        eventSource,
        eventVersion,
        userIdentity,
        awsRegion,
      } = record;
      if (
        !dynamodb ||
        !eventID ||
        !eventName ||
        !eventSource ||
        !eventVersion ||
        !awsRegion
      ) {
        return acc;
      }

      if (
        eventName !== "INSERT" &&
        eventName !== "MODIFY" &&
        eventName !== "REMOVE"
      ) {
        return acc;
      }

      const { ApproximateCreationDateTime, StreamViewType } = dynamodb;

      if (
        StreamViewType !== "NEW_AND_OLD_IMAGES" &&
        StreamViewType !== "NEW_IMAGE" &&
        StreamViewType !== "OLD_IMAGE" &&
        StreamViewType !== "KEYS_ONLY"
      ) {
        return acc;
      }

      acc.push({
        dynamodb: {
          ApproximateCreationDateTime: ApproximateCreationDateTime
            ? Math.floor(ApproximateCreationDateTime.getTime() / 1000)
            : undefined,
          Keys: dynamodb.Keys as { [key: string]: any },
          NewImage: dynamodb.NewImage as { [key: string]: any },
          OldImage: dynamodb.OldImage as { [key: string]: any },
          SequenceNumber: dynamodb.SequenceNumber,
          SizeBytes: dynamodb.SizeBytes,
          StreamViewType,
        },
        eventID,
        eventName,
        eventSource,
        eventVersion,
        userIdentity,
        awsRegion,
        eventSourceARN: this.streamArn,
      });

      return acc;
    }, [] as DynamoDBRecord[]);
  }

  stringify = (): string => {
    return JSON.stringify({ Records: this.Records });
  };

  hasRecords = (): boolean => {
    return this.Records.length > 0;
  };
}
