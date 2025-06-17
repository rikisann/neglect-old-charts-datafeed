import { Router } from "express";
import { Bar } from "../types/Bar";
import { db } from "../db";
import { Swap } from "@prisma/client";

const router = Router();

router.get("/", async (req, res) => {
  console.log("History request received");
  const address = req.query.address as string;
  const from = parseInt(req.query.from as string, 10);
  const to = parseInt(req.query.to as string, 10);
  const resolutionParam = req.query.resolution as string;
  const countBack = req.query.countBack ? parseInt(req.query.countBack as string, 10) : null;

  let resolutionMs: number;
  if (resolutionParam.endsWith("S") || resolutionParam.endsWith("s")) {
    // Resolution is in seconds
    const seconds = parseInt(resolutionParam.slice(0, -1), 10);
    resolutionMs = seconds * 1000;
  } else {
    // Resolution is in minutes
    const minutes = parseInt(resolutionParam, 10);
    resolutionMs = minutes * 60 * 1000;
  }

  console.log("Fetching transactions for ", address, from, to, resolutionMs, countBack);
  const start = new Date();
  // let transactions = await db.swap.findMany({
  //   where: {
  //     tokenAddress: address,
  //     timestamp: {
  //       gte: countBack ? undefined : new Date(from * 1000),
  //       lte: new Date(to * 1000),
  //     },
  //     totalUsd: {
  //       gt: 1,
  //     },
  //   },
  //   select: {
  //     totalUsd: true,
  //     tokenAmount: true,
  //     price: true,
  //     timestamp: true,

  //   },
  //   orderBy: {
  //     timestamp: "desc",
  //   },
  // });

  const query = `
  SELECT 
    "totalUsd", 
    "tokenAmount", 
    "price", 
    "timestamp"
  FROM "Swap"
  WHERE 
    "tokenAddress" = $1
    AND "timestamp" <= TIMESTAMP 'epoch' + $2 * INTERVAL '1 second'
    ${countBack ? '' : 'AND "timestamp" >= TIMESTAMP \'epoch\' + $3 * INTERVAL \'1 second\''}
    AND "totalUsd" > 1
  ORDER BY "timestamp" DESC
`;

  const queryParams = countBack
    ? [address, to]
    : [address, to, from];

  const transactions = await db.$queryRawUnsafe(query, ...queryParams) as Swap[];


  console.log("Fetched ", transactions.length, " transactions for ", address, "in", new Date().getTime() - start.getTime(), "ms");

  if (transactions.length === 0) {
    console.log("No transactions found for ", address);
    res.json({ bars: [], noData: true });
    return
  }

  const barsMap: { [key: number]: Bar } = {};

  for (const transaction of transactions) {
    try {
      const price = transaction.price;
      const time = new Date(transaction.timestamp);
      const bucket = Math.floor(time.getTime() / resolutionMs) * resolutionMs;

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

    if (countBack && Object.keys(barsMap).length >= countBack) {
      break
    }
  }

  // Convert to array and sort chronologically (oldest first)
  let barsArray = Object.values(barsMap);
  barsArray.sort((a, b) => a.time - b.time);

  // Set open prices correctly
  for (let i = 0; i < barsArray.length; i++) {
    if (i === 0) {
      barsArray[i].open = barsArray[i].low;
    } else {
      barsArray[i].open = barsArray[i - 1].close;
    }
  }

  // If countBack is specified, only return that many bars
  if (countBack && barsArray.length > countBack) {
    // Keep only the newest 'countBack' bars
    barsArray = barsArray.slice(barsArray.length - countBack);
  }

  // Check if we have the requested number of bars
  const hasEnoughBars = countBack ? barsArray.length >= countBack : true;
  const noMoreData = !hasEnoughBars;

  console.log(`Sending ${barsArray.length} bars for ${address}`);
  console.log(`Requested: ${countBack} bars, Has enough: ${hasEnoughBars}, No more data: ${noMoreData}\n`);

  res.json({
    bars: barsArray,
    noData: noMoreData
  });
});

export default router;
