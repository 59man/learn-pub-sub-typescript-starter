import type amqp from "amqplib";
import { decode } from "@msgpack/msgpack";
import { declareAndBind, SimpleQueueType } from "./declareAndBind.js";

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
): Promise<void> {
  const [channel, queue] = await declareAndBind(conn, exchange, queueName, routingKey, simpleQueueType);
  await channel.prefetch(10);
  await channel.consume(queue.queue, async (message) => {
    if (message === null) return;
    try {
      const data = deserializer(message.content);
      const ackType = await handler(data);
      if (ackType === AckType.Ack) {
        console.log("Ack");
        channel.ack(message);
      } else if (ackType === AckType.NackRequeue) {
        console.log("NackRequeue");
        channel.nack(message, false, true);
      } else {
        console.log("NackDiscard");
        channel.nack(message, false, false);
      }
    } catch (err) {
      console.error("Error processing message:", err);
      channel.nack(message, false, false);
    }
  });
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, key, queueType, handler, (data) => JSON.parse(data.toString()) as T);
}

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, key, queueType, handler, (data) => decode(data) as T);
}
