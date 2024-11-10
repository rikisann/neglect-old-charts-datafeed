// server.ts

import cors from "cors";
import { createServer } from "http";
import express from "express";
import historyRouter from "./routes/history";
import { initializeWebSocket } from "./websocketHandler";

const app = express();
const port = 3001;
const server = createServer(app);

app.use(cors({ origin: "*" }));
app.use("/history", historyRouter);

initializeWebSocket(server, port);
