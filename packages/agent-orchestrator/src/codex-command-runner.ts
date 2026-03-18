import { stat } from "node:fs/promises";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";

export interface RunCodexCommandOptions {
  artifactPollIntervalMs: number;
  commandTimeoutMs: number;
  onStderrChunk?: (chunk: string) => void;
  onStdoutChunk?: (chunk: string) => void;
  processExitGracePeriodMs: number;
}

export async function runCodexCommand(
  codexCommand: string,
  args: string[],
  prompt: string,
  cwd: string,
  requiredArtifacts: string[],
  options: RunCodexCommandOptions
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(codexCommand, args, {
      cwd,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let settled = false;
    let stderr = "";
    let stdout = "";

    const artifactPoll = setInterval(() => {
      void checkArtifacts();
    }, options.artifactPollIntervalMs);
    const commandTimeout = setTimeout(() => {
      finishWithError(
        new Error(
          [
            `Codex CLI timed out after ${options.commandTimeoutMs}ms.`,
            stderr && `stderr: ${stderr.trim()}`,
            stdout && `stdout: ${stdout.trim()}`,
          ]
            .filter(Boolean)
            .join(" ")
        )
      );
    }, options.commandTimeoutMs);

    function cleanup(): void {
      clearInterval(artifactPoll);
      clearTimeout(commandTimeout);
    }

    function finishWithSuccess(): void {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();
      void stopChildProcess(child, options.processExitGracePeriodMs);
      resolve();
    }

    function finishWithError(error: Error): void {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();
      void stopChildProcess(child, options.processExitGracePeriodMs);
      reject(error);
    }

    async function checkArtifacts(): Promise<void> {
      if (settled) {
        return;
      }

      if (await areArtifactsReady(requiredArtifacts)) {
        finishWithSuccess();
      }
    }

    child.stderr.on("data", (chunk: Buffer | string) => {
      const text = chunk.toString();
      stderr += text;
      options.onStderrChunk?.(text);
    });

    child.stdout.on("data", (chunk: Buffer | string) => {
      const text = chunk.toString();
      stdout += text;
      options.onStdoutChunk?.(text);
    });

    child.on("error", (error) => {
      finishWithError(error);
    });

    child.on("close", (code) => {
      if (settled) {
        return;
      }

      if (code === 0) {
        void checkArtifacts().then(() => {
          if (!settled) {
            finishWithError(
              new Error(
                "Codex CLI exited successfully without writing the required artifacts."
              )
            );
          }
        });
        return;
      }

      finishWithError(
        new Error(stderr || `Codex CLI exited with code ${code ?? "unknown"}.`)
      );
    });

    child.stdin.write(prompt);
    child.stdin.end();
    void checkArtifacts();
  });
}

export async function readRequiredArtifact(filePath: string): Promise<string> {
  const fileStat = await stat(filePath);

  if (!fileStat.isFile() || fileStat.size === 0) {
    throw new Error(
      `Expected Codex to write a non-empty artifact at ${filePath}.`
    );
  }

  const { readFile } = await import("node:fs/promises");
  return await readFile(filePath, "utf8");
}

async function areArtifactsReady(filePaths: string[]): Promise<boolean> {
  const statuses = await Promise.all(
    filePaths.map(async (filePath) => {
      try {
        const fileStat = await stat(filePath);
        return fileStat.isFile() && fileStat.size > 0;
      } catch {
        return false;
      }
    })
  );

  return statuses.every(Boolean);
}

async function stopChildProcess(
  child: ChildProcessWithoutNullStreams,
  processExitGracePeriodMs: number
): Promise<void> {
  if (child.killed || child.exitCode !== null) {
    return;
  }

  child.kill("SIGTERM");

  await new Promise<void>((resolve) => {
    const forceKillTimeout = setTimeout(() => {
      if (!child.killed && child.exitCode === null) {
        child.kill("SIGKILL");
      }

      resolve();
    }, processExitGracePeriodMs);

    child.once("close", () => {
      clearTimeout(forceKillTimeout);
      resolve();
    });
  });
}
