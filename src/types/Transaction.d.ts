export interface Transaction {
  type: string;
  signature: string;
  tokenAmount: number;
  lamportsAmount: number;
  maker: string;
  totalUsd: number;
  timestamp: number;
}
