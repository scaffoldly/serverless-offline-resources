const {
  DescribeStreamCommand,
  GetRecordsCommand,
  GetShardIteratorCommand,
} = require("@aws-sdk/client-dynamodb-streams");

const { assign } = require("lodash/fp");

class DynamoDBStreamPoller {
  constructor(client, streamArn, functions = []) {
    this.client = client;
    this.streamArn = streamArn;
    this.functions = functions;
    this.shardIterators = new Map();
    this.timeoutIds = new Map();
    this.recordQueues = new Map();
  }

  async start() {
    try {
      const {
        StreamDescription: { Shards },
      } = await this.client.send(
        new DescribeStreamCommand({ StreamArn: this.streamArn })
      );

      for (const shard of Shards) {
        const { ShardIterator } = await this.client.send(
          new GetShardIteratorCommand({
            ShardId: shard.ShardId,
            ShardIteratorType: "LATEST",
            StreamArn: this.streamArn,
            SequenceNumber: shard.SequenceNumberRange.StartingSequenceNumber,
          })
        );
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

  async getRecords(shardId) {
    try {
      const shardIterator = this.shardIterators.get(shardId);
      if (!shardIterator) return;

      const { Records, NextShardIterator } = await this.client.send(
        new GetRecordsCommand({ ShardIterator: shardIterator })
      );

      const recordQueue = this.recordQueues.get(shardId);
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

            if (
              functionConfig.maximumRecordAgeInSeconds !== null &&
              functionConfig.maximumRecordAgeInSeconds !== undefined
            ) {
              const now = Date.now();
              filteredRecords = filteredRecords.filter((record) => {
                const recordAgeInSeconds =
                  (now -
                    new Date(
                      record.dynamodb.ApproximateCreationDateTime * 1000
                    ).getTime()) /
                  1000;
                return (
                  recordAgeInSeconds <= functionConfig.maximumRecordAgeInSeconds
                );
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

class StreamEvent {
  constructor(Records, region, streamArn) {
    this.Records = Records.map(
      assign({
        eventSourceARN: streamArn,
        awsRegion: region,
      })
    );
  }
}

module.exports = { DynamoDBStreamPoller, StreamEvent };
