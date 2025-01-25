import { Router } from "express";
import { redis } from "../connection";
import { Bar } from "../types/Bar";
import { db } from "../db";

const router = Router();

router.get("/", async (req, res) => {
  console.log("History request received");
  const address = req.query.address as string;
  const from = parseInt(req.query.from as string, 10);
  const to = parseInt(req.query.to as string, 10);
  const decimals = parseInt(req.query.decimals as string, 10) || 6;
  const resolutionParam = req.query.resolution as string;

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

  console.log("Fetching transactions for ", address, from, to, resolutionMs);
  const transactions = await db.transaction.findMany({
    where: {
      tokenAddress: address,
      timestamp: {
        gte: new Date(from * 1000),
        lte: new Date(to * 1000),
      },
    },
    select: {
      tokenAmount: true,
      totalUsd: true,
      timestamp: true,
    },
    orderBy: {
      timestamp: "asc",
    },
  });
  console.log("Fetched ", transactions.length, " transactions for ", address);

  console.log("Fetching previous transactions for ", address, " ", new Date(from * 1000));
  const previousTransactions = await db.transaction.findFirst({
    where: {
      tokenAddress: address,
      timestamp: {
        lt: new Date(from * 1000),
      },
    },
    select: {
      tokenAmount: true,
      totalUsd: true,
    },
    orderBy: {
      timestamp: "desc",
    },
  });
  console.log("Fetched previous transactions for ", address);

  let previousClosePrice: number | undefined;
  if (previousTransactions) {
    if (
      previousTransactions.tokenAmount > 0 &&
      previousTransactions.totalUsd > 0
    ) {
      const tokenAmount = previousTransactions.tokenAmount / 10 ** decimals;
      previousClosePrice = previousTransactions.totalUsd / tokenAmount;
    }
  }

  if (transactions.length === 0) {
    console.log("No transactions found for ", address);
    res.json({ bars: [], noData: true });
    return;
  }

  const barsMap: { [key: number]: Bar } = {};
  console.log("Processing ", transactions.length, " transactions");
  transactions.forEach((transaction) => {
    try {
      if (transaction.tokenAmount <= 0 || transaction.totalUsd <= 0) return;

      const tokenAmount = transaction.tokenAmount / 10 ** decimals;

      const price = transaction.totalUsd / tokenAmount;
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
  });

  const barsArray = Object.values(barsMap);
  barsArray.sort((a, b) => a.time - b.time);
  for (let i = 0; i < barsArray.length; i++) {
    if (i === 0) {
      barsArray[i].open = previousClosePrice || barsArray[i].low;
    } else {
      barsArray[i].open = barsArray[i - 1].close;
    }
  }

  console.log("Sending bars ", barsArray.length, " for ", address);
  res.json({ bars: barsArray, noData: false });
});

export default router;
