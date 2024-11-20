import { Router } from "express";
import { redis } from "../connection";
import { Bar } from "../types/Bar";

const router = Router();

router.get("/", async (req, res) => {
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

  const transactionsData = await redis.zrangebyscore(
    `transactions:${address}`,
    from,
    to
  );

  const previousTransactionsData = await redis.zrangebyscore(
    `transactions:${address}`,
    from - 1,
    "-inf",
    "LIMIT",
    0,
    1
  );
  let previousClosePrice: number | undefined;
  if (previousTransactionsData.length > 0) {
    const previousTransaction = JSON.parse(previousTransactionsData[0]);
    if (
      previousTransaction.tokenAmount > 0 &&
      previousTransaction.totalUsd > 0
    ) {
      const tokenAmount = previousTransaction.tokenAmount / 10 ** decimals;
      previousClosePrice = previousTransaction.totalUsd / tokenAmount;
    }
  }

  if (transactionsData.length === 0) {
    res.json({ bars: [], noData: true });
    return;
  }

  const transactions = transactionsData.map((transaction) =>
    JSON.parse(transaction)
  );

  const barsMap: { [key: number]: Bar } = {};
  transactions.forEach((transaction) => {
    try {
      if (transaction.tokenAmount <= 0 || transaction.totalUsd <= 0) return;

      const tokenAmount = transaction.tokenAmount / 10 ** decimals;

      const price = transaction.totalUsd / tokenAmount;
      const time = new Date(transaction.timestamp * 1000);
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
  res.json({ bars: barsArray, noData: false });
});

export default router;
