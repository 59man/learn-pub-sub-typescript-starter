import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const body = Buffer.from(JSON.stringify(value));
  return new Promise((resolve, reject) => {
    ch.publish(exchange, routingKey, body, { contentType: "application/json" }, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const body = Buffer.from(encode(value));
  return new Promise((resolve, reject) => {
    ch.publish(exchange, routingKey, body, { contentType: "application/x-msgpack" }, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}