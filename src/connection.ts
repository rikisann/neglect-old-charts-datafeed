import { Redis } from "ioredis";
import dotenv from "dotenv";

dotenv.config();

export const redis = new Redis({
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT),
  username: process.env.REDIS_USERNAME,
  password: process.env.REDIS_PASSWORD,
  tls: {
    host: process.env.REDIS_HOST,
  },
});

// export const redis = new Redis({
//   host: "localhost",
//   port: 6379,
// });

export const redisSubscriber = redis.duplicate();
redisSubscriber.on("error", (err: Error) => {
  console.error("Redis Client Error", err);
});
