import { Router } from "express";
import { redis } from "../connection";
import { Bar } from "../types/Bar";

const router = Router();

router.get("/", async (req, res) => {
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

export default router;
