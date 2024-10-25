// server.ts

import express, { Request, Response } from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import Redis from "ioredis";
import cors from "cors";
import { redis, redisSubscriber } from "./connection";
import { aggregateTransactionsToBars } from "./lib/aggregateTransactionsToBars";
import {
  getTransactionsBeforeTimestamp,
  resolutionToSeconds,
} from "./lib/helpers";
import { Bar } from "./types/Bar";

const app = express();
const port = 3001;
const server = createServer(app);
const wss = new WebSocketServer({ server });

app.use(cors({ origin: "*" }));

redisSubscriber.on("error", (err: Error) => {
  console.error("Redis Client Error", err);
});

// Keep track of connected clients
const clients = new Set<WebSocket>();

// WebSocket connection
wss.on("connection", (ws: WebSocket) => {
  console.log("Client connected");
  clients.add(ws);

  ws.on("close", () => {
    console.log("Client disconnected");
    clients.delete(ws);
  });
});

(async () => {
  try {
    await redisSubscriber.subscribe("newSwap");

    redisSubscriber.on("message", (channel, message) => {
      if (channel === "newSwap") {
        clients.forEach((client) => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(message);
          }
        });
      }
    });

    server.listen(port, () => {
      console.log(`Data feed server running on port ${port}`);
    });
  } catch (err) {
    console.error("Error starting server:", err);
  }
})();

app.get("/history", async (req: Request, res: Response) => {
  const address = req.query.address as string;
  const from = parseInt(req.query.from as string, 10);
  const to = parseInt(req.query.to as string, 10);
  const resolution = parseInt(req.query.resolution as string, 10);

  const transactionsData = await redis.zrangebyscore(
    `transactions:${address}`,
    from,
    to
  );

  const transactions = transactionsData.map((transaction) =>
    JSON.parse(transaction)
  );

  const barsMap: { [key: number]: Bar } = {};
  transactions.forEach((transaction) => {
    try {
      if (transaction.tokenAmount <= 0 || transaction.totalUsd <= 0) return;

      const tokenAmount = transaction.tokenAmount / 10 ** 6;

      const price = transaction.totalUsd / tokenAmount;
      const time = new Date(transaction.timestamp * 1000);
      const bucket =
        Math.floor(time.getTime() / (resolution * 60 * 1000)) *
        (resolution * 60 * 1000);

      if (!barsMap[bucket]) {
        barsMap[bucket] = {
          time: bucket,
          open: price,
          high: price,
          low: price,
          close: price,
          volume: transaction.totalUsd,
        };
      } else {
        const bar = barsMap[bucket];
        bar.high = Math.max(bar.high, price);
        bar.low = Math.min(bar.low, price);
        bar.close = price;
        bar.volume += transaction.totalUsd;
      }
    } catch (err) {
      console.error("Error processing transaction:", err);
    }
  });

  res.json(Object.values(barsMap));
});

// const address = req.query.address as string;
// const from = parseInt(req.query.from as string, 10);
// const to = parseInt(req.query.to as string, 10);

// const resolution = req.query.resolution as string;

// // Fetch historical data from Redis
// const transactionsData = await redis.zrangebyscore(
//   `transactions:${address}`,
//   from,
//   to - 1
// );

// let transactions = transactionsData.map((data: string) => JSON.parse(data));

// const interval = resolutionToSeconds(resolution);
// let bars = aggregateTransactionsToBars(transactions, interval);
// console.log(bars);

// let earliestTimestamp = from;

// while (bars.length < countBack) {
//   // Fetch earlier transactions
//   const additionalTransactions = await getTransactionsBeforeTimestamp(
//     address,
//     earliestTimestamp,
//     1000 // Fetch up to 1000 transactions at a time
//   );

//   if (additionalTransactions.length === 0) {
//     // No more data available
//     break;
//   }

//   // Prepend the additional transactions
//   transactions = additionalTransactions.concat(transactions);

//   // Update the earliestTimestamp
//   earliestTimestamp =
//     additionalTransactions[additionalTransactions.length - 1].timestamp;

//   // Re-aggregate transactions into bars
//   bars = aggregateTransactionsToBars(transactions, interval);
// }

// if (bars.length) {
//   if (bars.length < countBack) {
//     res.json({ bars, noData: true });
//   } else {
//     res.json({ bars, noData: false });
//   }
// } else {
//   res.json({ bars: [], noData: true });
// }
// } catch (err) {
//   console.error("Error fetching historical data:", err);
//   res.status(500).send("Internal Server Error");
// }
