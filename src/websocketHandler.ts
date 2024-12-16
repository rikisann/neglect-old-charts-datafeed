import { Server } from "http";
import { redisSubscriber } from "./connection";
import { WebSocket, WebSocketServer } from "ws";

const clients = new Map<WebSocket, Set<string>>();
const subscriptionMap = new Map<string, Set<WebSocket>>();

const initializeRedisSubscriber = async () => {
  await redisSubscriber.subscribe("newSwap");

  redisSubscriber.on("message", (channel, message) => {
    if (channel === "newSwap") {
      const tokenData = JSON.parse(message);
      if (!tokenData.transaction) return

      const latestTransaction = tokenData.transaction;
      const swap = {
        volume: latestTransaction.totalUsd,
        address: tokenData.address,
        price: tokenData.price,
        time: new Date(latestTransaction.timestamp).getTime(),
      };

      // Get all clients subscribed to this token
      const subscribers = subscriptionMap.get(tokenData.address);

      if (subscribers) {
        subscribers.forEach((ws) => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(swap));
          }
        });
      }
    }
  });
};

export const initializeWebSocket = async (server: Server, port: number) => {
  try {
    const wss = new WebSocketServer({ server });

    wss.on("connection", (ws: WebSocket) => {
      console.log("Client connected");
      clients.set(ws, new Set());

      ws.on("message", (message) => {
        try {
          const messageString = message.toString();
          const parsedMessage = JSON.parse(messageString);
          const { action, subs } = parsedMessage as {
            action: string;
            subs: string[];
          };

          if (action === "SubAdd") {
            subs.forEach((sub: string) => {
              // Add sub to client's set
              clients.get(ws).add(sub);

              // Add ws to subscriptionMap under sub
              if (!subscriptionMap.has(sub)) {
                subscriptionMap.set(sub, new Set());
              }
              subscriptionMap.get(sub).add(ws);
            });
          }

          if (action === "SubRemove") {
            subs.forEach((sub: string) => {
              // Remove sub from client's set
              clients.get(ws).delete(sub);

              // Remove ws from subscriptionMap under sub
              if (subscriptionMap.has(sub)) {
                subscriptionMap.get(sub).delete(ws);

                // Clean up if no clients left subscribed to sub
                if (subscriptionMap.get(sub).size === 0) {
                  subscriptionMap.delete(sub);
                }
              }
            });
          }
        } catch (error) {
          console.error("Error parsing message:", error);
        }
      });

      ws.on("close", () => {
        const clientSubs = clients.get(ws);

        // Remove the client from subscriptionMap for each sub
        clientSubs.forEach((sub) => {
          if (subscriptionMap.has(sub)) {
            subscriptionMap.get(sub).delete(ws);

            if (subscriptionMap.get(sub).size === 0) {
              subscriptionMap.delete(sub);
            }
          }
        });

        // Remove client from clients map
        clients.delete(ws);
      });
    });

    await initializeRedisSubscriber();

    server.listen(port, () => {
      console.log(`Data feed server running on port ${port}`);
    });
  } catch (err) {
    console.error("Error starting server:", err);
  }
};
