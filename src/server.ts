// server.ts

import fs from "fs";
import cors from "cors";
import { createServer } from "https";
import express from "express";
import historyRouter from "./routes/history";
import { initializeWebSocket } from "./websocketHandler";
const app = express();
const port = 443;
const server = createServer(
  {
    key: fs.readFileSync("./ssl/chart_solindex_xyz.key"),
    cert: fs.readFileSync("./ssl/chart_solindex_xyz.crt"),
  },
  app
);

app.use("/hello", (req, res) => {
  res.send("Hello World");
});
app.use(cors({ origin: "*" }));
app.use("/history", historyRouter);

initializeWebSocket(server, port);
