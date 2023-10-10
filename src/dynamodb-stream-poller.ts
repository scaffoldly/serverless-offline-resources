import {
  DescribeStreamCommand,
  DynamoDBStreamsClient,
  GetRecordsCommand,
  GetShardIteratorCommand,
} from "@aws-sdk/client-dynamodb-streams";

import { assign } from "lodash/fp";

export type DynamoDbFunctionDefinition = {
  functionName: string;
  batchSize: number;
  maximumRecordAgeInSeconds?: number;
  recordStreamHandler: (
    records: any[],
    functionName: string,
    streamArn: string
  ) => void;
};

export class DynamoDBStreamPoller {
  shardIterators: Map<string, string>;
  timeoutIds: Map<string, NodeJS.Timeout>;
  recordQueues: Map<string, any[]>;
  constructor(
    private client: DynamoDBStreamsClient,
    private streamArn: string,
    private functions: DynamoDbFunctionDefinition[] = []
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
        if (!shard.SequenceNumberRange || !shard.ShardId) {
          continue;
        }
        const { ShardIterator } = await this.client.send(
          new GetShardIteratorCommand({
            ShardId: shard.ShardId,
            ShardIteratorType: "LATEST",
            StreamArn: this.streamArn,
            SequenceNumber: shard.SequenceNumberRange.StartingSequenceNumber,
          })
        );

        if (!ShardIterator) continue;

        this.shardIterators.set(shard.ShardId, ShardIterator);
        this.recordQueues.set(shard.ShardId, []);
      }

      await Promise.all(
        Shards.map(async (shard) => {
          await this.getRecords(shard.ShardId);
        })
      );
    } catch (error) {
      console.warn(error);
    }
  }

  async getRecords(shardId: string | undefined) {
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
              filteredRecords = filteredRecords.filter((record) => {
                const recordAgeInSeconds =
                  (now -
                    new Date(
                      record.dynamodb.ApproximateCreationDateTime * 1000
                    ).getTime()) /
                  1000;
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
      } catch (handlerError) {
        console.warn(handlerError);
      }

      if (NextShardIterator) {
        this.shardIterators.set(shardId, NextShardIterator);
      }

      if (recordQueue.length > 0) {
        this.getRecords(shardId);
      } else if (NextShardIterator) {
        this.timeoutIds.set(
          shardId,
          setTimeout(() => this.getRecords(shardId), 1000)
        );
      }
    } catch (error) {
      this.timeoutIds.set(
        shardId,
        setTimeout(() => this.getRecords(shardId), 1000)
      );
    }
  }

  stop() {
    this.timeoutIds.forEach((timeoutId) => clearTimeout(timeoutId));
  }
}

export class StreamEvent {
  constructor(
    public Records: any[],
    public region: string,
    public streamArn: string
  ) {
    this.Records = Records.map(
      assign({
        eventSourceARN: streamArn,
        awsRegion: region,
      })
    );
  }
}
