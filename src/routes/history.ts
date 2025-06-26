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

  // Batch processing function that maintains the same logic as the original
  const fetchTransactionsInBatches = async (
    tokenAddress: string,
    toTimestamp: number,
    fromTimestamp?: number,
    targetBars?: number
  ): Promise<{ bars: Bar[], hasMoreData: boolean }> => {
    const barsMap: { [key: number]: Bar } = {};
    let currentTimestamp = toTimestamp;
    const batchSize = 5000; // Larger batch size
    let batchCount = 0;
    const maxBatches = 50; // Increased to allow more data fetching
    let totalTransactionsProcessed = 0;
    let shouldStop = false;

    while (batchCount < maxBatches && !shouldStop) {
      batchCount++;

      // Build query for this batch - same as original
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
          ${fromTimestamp ? 'AND "timestamp" >= TIMESTAMP \'epoch\' + $3 * INTERVAL \'1 second\'' : ''}
          AND "totalUsd" > 1
        ORDER BY "timestamp" DESC
        LIMIT ${batchSize}
      `;

      const queryParams = fromTimestamp 
        ? [tokenAddress, currentTimestamp, fromTimestamp]
        : [tokenAddress, currentTimestamp];

      console.log(`Batch ${batchCount}: Fetching transactions before ${new Date(currentTimestamp * 1000).toISOString()}`);
      
      const batchTransactions = await db.$queryRawUnsafe(query, ...queryParams) as Swap[];
      
      if (batchTransactions.length === 0) {
        console.log(`Batch ${batchCount}: No more transactions found`);
        break;
      }

      console.log(`Batch ${batchCount}: Processing ${batchTransactions.length} transactions`);
      
      // Process transactions in the SAME way as the original code
      for (const transaction of batchTransactions) {
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
            bar.close = price; // This maintains chronological order since we process DESC
            bar.volume += transaction.totalUsd;
          }

          totalTransactionsProcessed++;

          // Only stop early for countBack mode, not for time range mode
          if (targetBars && Object.keys(barsMap).length >= targetBars) {
            console.log(`Reached target of ${targetBars} bars after processing ${totalTransactionsProcessed} transactions`);
            shouldStop = true;
            break;
          }
        } catch (err) {
          console.error("Error processing transaction:", err);
        }
      }

      // Update timestamp for next batch if we haven't stopped
      if (!shouldStop && batchTransactions.length > 0) {
        const oldestTransaction = batchTransactions[batchTransactions.length - 1];
        currentTimestamp = Math.floor(new Date(oldestTransaction.timestamp).getTime() / 1000) - 1;
      }

      // Check stopping conditions
      const currentBarsCount = Object.keys(barsMap).length;
      console.log(`Batch ${batchCount} complete: ${currentBarsCount} bars created from ${totalTransactionsProcessed} transactions`);

      // For countBack mode: Stop if we have enough bars
      if (targetBars && currentBarsCount >= targetBars) {
        shouldStop = true;
        break;
      }

      // For both modes: Stop if we got fewer transactions than requested (end of data)
      if (batchTransactions.length < batchSize) {
        console.log(`Got ${batchTransactions.length} transactions, less than batch size ${batchSize}. End of data.`);
        shouldStop = true;
        break;
      }

      // For time range mode: Stop if we hit the from timestamp limit
      if (fromTimestamp && currentTimestamp <= fromTimestamp) {
        console.log(`Reached from timestamp limit: ${new Date(fromTimestamp * 1000).toISOString()}`);
        shouldStop = true;
        break;
      }

      // Safety check: Don't fetch forever for countBack mode without fromTimestamp
      if (!fromTimestamp && !targetBars && batchCount >= 20) {
        console.log(`Safety limit reached: ${batchCount} batches processed`);
        shouldStop = true;
        break;
      }
    }

    // Convert bars to array and sort - SAME as original
    const barsArray = Object.values(barsMap);
    barsArray.sort((a, b) => a.time - b.time);

    // Set open prices correctly - SAME as original
    for (let i = 0; i < barsArray.length; i++) {
      if (i === 0) {
        barsArray[i].open = barsArray[i].low;
      } else {
        barsArray[i].open = barsArray[i - 1].close;
      }
    }

    // Determine if there's more data
    const hasMoreData = targetBars ? barsArray.length >= targetBars : false;

    console.log(`Batch processing complete: ${batchCount} batches, ${totalTransactionsProcessed} transactions, ${barsArray.length} bars`);

    return { bars: barsArray, hasMoreData };
  };

  try {
    let result: { bars: Bar[], hasMoreData: boolean };

    if (countBack) {
      // For countBack mode, fetch until we have enough bars
      result = await fetchTransactionsInBatches(address, to, undefined, countBack);
      
      // SAME slicing logic as original
      if (result.bars.length > countBack) {
        result.bars = result.bars.slice(result.bars.length - countBack);
      }
    } else {
      // For time range mode, fetch all bars in the range
      result = await fetchTransactionsInBatches(address, to, from);
      result.hasMoreData = false;
    }

    console.log("Fetched and processed in", new Date().getTime() - start.getTime(), "ms");

    if (result.bars.length === 0) {
      console.log("No bars found for ", address);
      res.json({ bars: [], noData: true });
      return;
    }

    // SAME noData logic as original
    const hasEnoughBars = countBack ? result.bars.length >= countBack : true;
    const noMoreData = !hasEnoughBars;

    console.log(`Sending ${result.bars.length} bars for ${address}`);
    console.log(`Requested: ${countBack} bars, Has enough: ${hasEnoughBars}, No more data: ${noMoreData}\n`);

    res.json({
      bars: result.bars,
      noData: noMoreData
    });

  } catch (error) {
    console.error("Error in batch processing:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;