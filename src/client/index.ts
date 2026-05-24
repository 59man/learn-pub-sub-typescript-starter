import amqp from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  getMaliciousLog,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { SimpleQueueType } from "../internal/pubsub/declareAndBind.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, ArmyMovesPrefix, PauseKey, WarRecognitionsPrefix, GameLogSlug } from "../internal/routing/routing.js";
import { handlerPause, handlerMove, handlerWar } from "./handlers.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameLog } from "../internal/gamelogic/logs.js";
import type { ConfirmChannel } from "amqplib";

export async function publishGameLog(ch: ConfirmChannel, username: string, message: string): Promise<void> {
  const log: GameLog = {
    username,
    message,
    currentTime: new Date(),
  };
  await publishMsgPack(ch, ExchangePerilTopic, `${GameLogSlug}.${username}`, log);
}

async function main() {
  console.log("Starting Peril client...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully!");

  const username = await clientWelcome();
  const gs = new GameState(username);

  const publishChannel = await conn.createConfirmChannel();
  await publishChannel.assertExchange(ExchangePerilTopic, "topic", { durable: true });

  await subscribeJSON<PlayingState>(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gs),
  );
  console.log(`Queue pause.${username} declared and bound!`);

  await subscribeJSON<ArmyMove>(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gs, publishChannel),
  );
  console.log(`Queue ${ArmyMovesPrefix}.${username} declared and bound!`);

  await subscribeJSON<RecognitionOfWar>(
    conn,
    ExchangePerilTopic,
    WarRecognitionsPrefix,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    handlerWar(gs, publishChannel, publishGameLog),
  );
  console.log(`Queue ${WarRecognitionsPrefix} declared and bound!`);

  while (true) {
    const words = await getInput();
    if (words.length === 0) continue;

    const cmd = words[0];
    if (cmd === "spawn") {
      try {
        commandSpawn(gs, words);
      } catch (err: any) {
        console.error(err.message);
      }
    } else if (cmd === "move") {
      try {
        const move = commandMove(gs, words);
        await publishJSON(publishChannel, ExchangePerilTopic, `${ArmyMovesPrefix}.${username}`, move);
        console.log("Move published successfully!");
      } catch (err: any) {
        console.error(err.message);
      }
    } else if (cmd === "status") {
      await commandStatus(gs);
    } else if (cmd === "help") {
      printClientHelp();
    } else if (cmd === "spam") {
      const n = parseInt(words[1] ?? "", 10);
      if (isNaN(n)) {
        console.log("Usage: spam <n>");
      } else {
        for (let i = 0; i < n; i++) {
          const log: GameLog = {
            username,
            message: getMaliciousLog(),
            currentTime: new Date(),
          };
          await publishMsgPack(publishChannel, ExchangePerilTopic, `${GameLogSlug}.${username}`, log);
        }
        console.log(`Spammed ${n} logs!`);
      }
    } else if (cmd === "quit") {
      printQuit();
      break;
    } else {
      console.log(`Unknown command: ${cmd}`);
    }
  }

  await conn.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});