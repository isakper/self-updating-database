import type { IncomingMessage, ServerResponse } from "node:http";

import type { CodexRunEvent } from "../../../../packages/shared/src/index.js";

export interface CodexRunEventHub {
  handleSse(
    sourceDatasetId: string,
    request: IncomingMessage,
    response: ServerResponse<IncomingMessage>,
    initialEvents: CodexRunEvent[]
  ): void;
  publish(runEvent: CodexRunEvent): void;
}

export function createCodexRunEventHub(): CodexRunEventHub {
  const listeners = new Map<string, Set<ServerResponse<IncomingMessage>>>();

  return {
    handleSse(sourceDatasetId, request, response, initialEvents) {
      response.writeHead(200, {
        "cache-control": "no-cache, no-transform",
        connection: "keep-alive",
        "content-type": "text/event-stream; charset=utf-8",
      });

      initialEvents.forEach((runEvent) => {
        writeEvent(response, runEvent);
      });

      const datasetListeners = listeners.get(sourceDatasetId) ?? new Set();
      datasetListeners.add(response);
      listeners.set(sourceDatasetId, datasetListeners);

      request.on("close", () => {
        datasetListeners.delete(response);

        if (datasetListeners.size === 0) {
          listeners.delete(sourceDatasetId);
        }
      });
    },
    publish(runEvent) {
      const datasetListeners = listeners.get(runEvent.sourceDatasetId);

      if (!datasetListeners) {
        return;
      }

      datasetListeners.forEach((response) => {
        writeEvent(response, runEvent);
      });
    },
  };
}

function writeEvent(
  response: ServerResponse<IncomingMessage>,
  runEvent: CodexRunEvent
): void {
  response.write(`event: codex-run\n`);
  response.write(`data: ${JSON.stringify(runEvent)}\n\n`);
}
