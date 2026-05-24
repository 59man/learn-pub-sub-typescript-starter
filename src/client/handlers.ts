import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { AckType } from "../internal/pubsub/subscribe.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import type { ConfirmChannel } from "amqplib";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(
  gs: GameState,
  publishChannel: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove) => {
    const outcome = handleMove(gs, move);
    if (outcome === MoveOutcome.MakeWar) {
      const rw: RecognitionOfWar = {
        attacker: move.player,
        defender: gs.getPlayerSnap(),
      };
      try {
        await publishJSON(publishChannel, ExchangePerilTopic, `${WarRecognitionsPrefix}.${gs.getUsername()}`, rw);
      } catch {
        process.stdout.write("> ");
        return AckType.NackRequeue;
      }
      process.stdout.write("> ");
      return AckType.Ack;
    }
    process.stdout.write("> ");
    if (outcome === MoveOutcome.Safe) {
      return AckType.Ack;
    }
    return AckType.NackDiscard;
  };
}

export function handlerWar(
  gs: GameState,
  publishChannel: ConfirmChannel,
  publishGameLog: (ch: ConfirmChannel, username: string, message: string) => Promise<void>,
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar) => {
    const outcome = handleWar(gs, rw);
    process.stdout.write("> ");
    switch (outcome.result) {
      case WarOutcome.NotInvolved:
        return AckType.NackRequeue;
      case WarOutcome.NoUnits:
        return AckType.NackDiscard;
      case WarOutcome.YouWon:
      case WarOutcome.OpponentWon: {
        const msg = `${outcome.winner} won a war against ${outcome.loser}`;
        try {
          await publishGameLog(publishChannel, gs.getUsername(), msg);
        } catch (err) {
          console.error("publishGameLog failed:", err);
          return AckType.NackRequeue;
        }
        return AckType.Ack;
      }
      case WarOutcome.Draw: {
        const msg = `A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw`;
        try {
          await publishGameLog(publishChannel, gs.getUsername(), msg);
        } catch (err) {
          console.error("publishGameLog failed:", err);
          return AckType.NackRequeue;
        }
        return AckType.Ack;
      }
      default:
        console.error("Unknown war outcome:", outcome);
        return AckType.NackDiscard;
    }
  };
}
