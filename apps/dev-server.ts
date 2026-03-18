import { loadLocalEnvironment } from "./shared/load-env.js";
import { startApiServer } from "./api/src/runtime/server.js";
import { startWebServer } from "./web/src/runtime/server.js";

loadLocalEnvironment();

const api = await startApiServer({
  port: Number(process.env.API_PORT ?? "3001"),
});

const web = await startWebServer({
  apiBaseUrl: `http://127.0.0.1:${api.port}`,
  port: Number(process.env.WEB_PORT ?? "3000"),
});

console.log(`API server ready at http://127.0.0.1:${api.port}`);
console.log(`Web server ready at http://127.0.0.1:${web.port}`);

const shutdown = async () => {
  await Promise.all([web.close(), api.close()]);
  process.exit(0);
};

process.on("SIGINT", () => {
  void shutdown();
});

process.on("SIGTERM", () => {
  void shutdown();
});
