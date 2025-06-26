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

  // Get token info to optimize batch sizes
  const tokenInfo = await db.token.findUnique({
    where: { address },
    select: { txns: true }
  });

  const totalTxns = tokenInfo?.txns || 0;
  console.log(`Token has ${totalTxns} total transactions`);

  // Much more aggressive batch size calculation
  const calculateSmartBatchSize = (totalTransactions: number, targetBars?: number, timeRangeSeconds?: number): number => {
    if (totalTransactions === 0) return 1000;

    const resolutionMinutes = resolutionMs / (1000 * 60);

    // For very high activity tokens, use much larger batch sizes
    if (totalTransactions > 100000) {
      if (targetBars) {
        // Estimate based on time density
        // If we need 2316 bars at 5-minute resolution, that's ~193 hours of data
        const estimatedHours = (targetBars * resolutionMinutes) / 60;
        const estimatedTxPerHour = totalTransactions / (24 * 7); // Assume 1 week of data
        const estimatedTotalTx = estimatedHours * estimatedTxPerHour;

        // Use much larger batch to reduce round trips
        const smartBatchSize = Math.min(Math.max(estimatedTotalTx * 0.3, 10000), 50000);
        console.log(`Smart calculation: ${estimatedHours.toFixed(1)}h needed, ${estimatedTxPerHour.toFixed(0)} tx/h, batch: ${smartBatchSize}`);
        return Math.floor(smartBatchSize);
      } else {
        // For time range queries on high-activity tokens
        return Math.min(timeRangeSeconds ? timeRangeSeconds / 10 : 20000, 50000);
      }
    } else if (totalTransactions > 50000) {
      return targetBars ? Math.min(targetBars * 20, 15000) : 10000;
    } else if (totalTransactions > 10000) {
      return targetBars ? Math.min(targetBars * 10, 8000) : 5000;
    } else {
      return targetBars ? Math.min(targetBars * 5, 3000) : 2000;
    }
  };

  const timeRangeSeconds = to - from;
  const optimalBatchSize = calculateSmartBatchSize(totalTxns, countBack, timeRangeSeconds);
  console.log(`Using smart batch size: ${optimalBatchSize} for ${totalTxns} total txns`);

  // Enhanced batch processing with dynamic batch size adjustment
  const fetchTransactionsInBatches = async (
    tokenAddress: string,
    toTimestamp: number,
    fromTimestamp?: number,
    targetBars?: number
  ): Promise<{ bars: Bar[], hasMoreData: boolean }> => {
    const barsMap: { [key: number]: Bar } = {};
    let currentTimestamp = toTimestamp;
    let currentBatchSize = optimalBatchSize;
    let batchCount = 0;
    const maxBatches = 15; // Reduced max batches - force larger batches
    let totalTransactionsProcessed = 0;
    let shouldStop = false;
    let lastEfficiency = 0;

    console.log(`Starting smart batch processing with initial size ${currentBatchSize}, max batches: ${maxBatches}`);

    while (batchCount < maxBatches && !shouldStop) {
      batchCount++;

      // Adaptive batch sizing based on efficiency
      if (batchCount > 2 && lastEfficiency < 1.0 && currentBatchSize < 50000) {
        const newBatchSize = Math.min(currentBatchSize * 2, 50000);
        console.log(`Low efficiency (${lastEfficiency.toFixed(1)}%), increasing batch size from ${currentBatchSize} to ${newBatchSize}`);
        currentBatchSize = newBatchSize;
      }

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
        LIMIT ${currentBatchSize}
      `;

      const queryParams = fromTimestamp
        ? [tokenAddress, currentTimestamp, fromTimestamp]
        : [tokenAddress, currentTimestamp];

      console.log(`Batch ${batchCount}: Fetching ${currentBatchSize} transactions before ${new Date(currentTimestamp * 1000).toISOString()}`);

      const batchStart = Date.now();
      const batchTransactions = await db.$queryRawUnsafe(query, ...queryParams) as Swap[];
      const queryTime = Date.now() - batchStart;

      if (batchTransactions.length === 0) {
        console.log(`Batch ${batchCount}: No more transactions found`);
        break;
      }

      console.log(`Batch ${batchCount}: Got ${batchTransactions.length} transactions in ${queryTime}ms`);

      const processingStart = Date.now();
      const barsBeforeProcessing = Object.keys(barsMap).length;

      // Process transactions
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
            bar.close = price;
            bar.volume += transaction.totalUsd;
          }

          totalTransactionsProcessed++;

          // Early exit for countBack mode
          if (targetBars && Object.keys(barsMap).length >= targetBars) {
            console.log(`✓ Reached target of ${targetBars} bars after processing ${totalTransactionsProcessed} transactions`);
            shouldStop = true;
            break;
          }
        } catch (err) {
          console.error("Error processing transaction:", err);
        }
      }

      const processingTime = Date.now() - processingStart;

      // Update timestamp for next batch
      if (!shouldStop && batchTransactions.length > 0) {
        const oldestTransaction = batchTransactions[batchTransactions.length - 1];
        currentTimestamp = Math.floor(new Date(oldestTransaction.timestamp).getTime() / 1000) - 1;
      }

      const currentBarsCount = Object.keys(barsMap).length;
      const barsAdded = currentBarsCount - barsBeforeProcessing;
      const batchEfficiency = batchTransactions.length > 0 ? (barsAdded / batchTransactions.length * 100) : 0;
      lastEfficiency = totalTransactionsProcessed > 0 ? (currentBarsCount / totalTransactionsProcessed * 100) : 0;

      console.log(`Batch ${batchCount}: +${barsAdded} bars (${batchEfficiency.toFixed(1)}% batch efficiency, ${lastEfficiency.toFixed(1)}% overall) | Query: ${queryTime}ms, Process: ${processingTime}ms`);

      // Stopping conditions
      if (targetBars && currentBarsCount >= targetBars) {
        shouldStop = true;
        break;
      }

      if (batchTransactions.length < currentBatchSize * 0.8) {
        console.log(`End of data: got ${batchTransactions.length} < ${currentBatchSize * 0.8}`);
        shouldStop = true;
        break;
      }

      if (fromTimestamp && currentTimestamp <= fromTimestamp) {
        console.log(`Reached from timestamp limit: ${new Date(fromTimestamp * 1000).toISOString()}`);
        shouldStop = true;
        break;
      }

      // Emergency brake for very low efficiency
      if (batchCount >= 5 && lastEfficiency < 0.1) {
        console.log(`⚠️ Emergency stop: efficiency too low (${lastEfficiency.toFixed(2)}%)`);
        shouldStop = true;
        break;
      }
    }

    // Convert bars to array and sort
    const barsArray = Object.values(barsMap);
    barsArray.sort((a, b) => a.time - b.time);

    // Set open prices correctly
    for (let i = 0; i < barsArray.length; i++) {
      if (i === 0) {
        barsArray[i].open = barsArray[i].low;
      } else {
        barsArray[i].open = barsArray[i - 1].close;
      }
    }

    const hasMoreData = targetBars ? barsArray.length >= targetBars : false;
    const finalEfficiency = totalTransactionsProcessed > 0 ? (barsArray.length / totalTransactionsProcessed * 100) : 0;

    console.log(`🏁 Batch processing complete: ${batchCount} batches, ${totalTransactionsProcessed} transactions, ${barsArray.length} bars (${finalEfficiency.toFixed(1)}% final efficiency)`);

    return { bars: barsArray, hasMoreData };
  };

  try {
    let result: { bars: Bar[], hasMoreData: boolean };

    if (countBack) {
      result = await fetchTransactionsInBatches(address, to, undefined, countBack);

      if (result.bars.length > countBack) {
        result.bars = result.bars.slice(result.bars.length - countBack);
      }
    } else {
      result = await fetchTransactionsInBatches(address, to, from);
      result.hasMoreData = false;
    }

    console.log("✅ Fetched and processed in", new Date().getTime() - start.getTime(), "ms");

    if (result.bars.length === 0) {
      console.log("No bars found for ", address);
      res.json({ bars: [], noData: true });
      return;
    }

    const hasEnoughBars = countBack ? result.bars.length >= countBack : true;
    const noMoreData = !hasEnoughBars;

    console.log(`📊 Sending ${result.bars.length} bars for ${address}`);
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