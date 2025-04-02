import { Router } from "express";
import { Bar } from "../types/Bar";
import { db } from "../db";

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
  let transactions = await db.swap.findMany({
    where: {
      tokenAddress: address,
      timestamp: {
        gte: countBack ? undefined : new Date(from * 1000),
        lte: new Date(to * 1000),
      },
      tokenAmount: {
        gt: 1,
      },
    },
    select: {
      totalUsd: true,
      tokenAmount: true,
      price: true,
      timestamp: true,
    },
    // orderBy: {
    //   timestamp: "asc",
    // },
    take: countBack ? countBack : undefined,
  });

  console.log("Fetched ", transactions.length, " transactions for ", address);

  // transactions = countBack ? transactions.reverse() : transactions;

  if (transactions.length === 0) {
    console.log("No transactions found for ", address);
    res.json({ bars: [], noData: true });
    return
  }

  // console.log("Fetching previous transactions for ", address, " ", new Date(from * 1000));
  // const previousTransactions = await db.swap.findFirst({
  //   where: {
  //     tokenAddress: address,
  //     timestamp: {
  //       lt: new Date(from * 1000),
  //     },
  //     tokenAmount: {
  //       gt: 1,
  //     },
  //   },
  //   select: {
  //     tokenAmount: true,
  //     totalUsd: true,
  //     price: true,
  //   },
  //   orderBy: {
  //     timestamp: "desc",
  //   },
  // });
  // console.log("Fetched previous transactions for ", address);

  // let previousClosePrice = transactions[0].price;



  const barsMap: { [key: number]: Bar } = {};
  console.log("Processing ", transactions.length, " transactions");
  transactions.forEach((transaction) => {
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
  });

  const barsArray = Object.values(barsMap);
  barsArray.sort((a, b) => a.time - b.time);
  for (let i = 0; i < barsArray.length; i++) {
    if (i === 0) {
      barsArray[i].open = barsArray[i].low;
    } else {
      barsArray[i].open = barsArray[i - 1].close;
    }
  }

  console.log("Sending bars ", barsArray.length, " for ", address);

  if (transactions.length < countBack) {
    res.json({ bars: barsArray, noData: true });
  } else {
    res.json({ bars: barsArray, noData: false });
  }
});

export default router;
