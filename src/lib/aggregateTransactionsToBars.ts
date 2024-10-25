import { Bar } from "../types/Bar";

export function aggregateTransactionsToBars(
  transactions: any[],
  interval: number
): Bar[] {
  const barsMap: { [key: number]: Bar } = {};

  transactions.forEach((tx) => {
    const txTime = Math.floor(new Date(tx.timestamp).getTime() / 1000);
    const bucket = Math.floor(txTime / interval) * interval;

    const price = tx.totalUsd / tx.tokenAmount;
    const volume = tx.tokenAmount;

    if (!barsMap[bucket]) {
      barsMap[bucket] = {
        time: bucket * 1000,
        open: price,
        high: price,
        low: price,
        close: price,
        volume: volume,
      };
    } else {
      const bar = barsMap[bucket];
      bar.high = Math.max(bar.high, price);
      bar.low = Math.min(bar.low, price);
      bar.close = price;
      bar.volume += volume;
    }
  });

  return Object.values(barsMap);
}
