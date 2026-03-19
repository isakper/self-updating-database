import { stat } from "node:fs/promises";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";

export type CodexCommandFailureCode =
  | "missing_artifacts"
  | "process_exit"
  | "startup_failure"
  | "timeout";

export class CodexCommandError extends Error {
  readonly artifactIssues: string[] | null;
  readonly code: CodexCommandFailureCode;
  readonly elapsedMs: number;
  readonly stderr: string;
  readonly stdout: string;

  constructor(options: {
    artifactIssues?: string[];
    code: CodexCommandFailureCode;
    elapsedMs: number;
    message: string;
    stderr: string;
    stdout: string;
  }) {
    super(options.message);
    this.name = "CodexCommandError";
    this.code = options.code;
    this.elapsedMs = options.elapsedMs;
    this.stderr = options.stderr;
    this.stdout = options.stdout;
    this.artifactIssues = options.artifactIssues ?? null;
  }
}

export interface RequiredArtifact {
  filePath: string;
  validateContents?: (contents: string) => void;
}

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
  requiredArtifacts: RequiredArtifact[],
  options: RunCodexCommandOptions
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(codexCommand, args, {
      cwd,
      stdio: ["pipe", "pipe", "pipe"],
    });
    const startedAtMs = Date.now();
    let settled = false;
    let stderr = "";
    let stdout = "";

    const artifactPoll = setInterval(() => {
      void checkArtifacts();
    }, options.artifactPollIntervalMs);
    const commandTimeout = setTimeout(() => {
      void (async () => {
        const artifactValidation =
          await validateRequiredArtifacts(requiredArtifacts);
        const failureCode = inferTimeoutFailureCode(stderr, stdout);
        finishWithError(
          new CodexCommandError({
            artifactIssues: artifactValidation.issues,
            code: failureCode,
            elapsedMs: Date.now() - startedAtMs,
            message: buildTimeoutMessage({
              artifactIssues: artifactValidation.issues,
              commandTimeoutMs: options.commandTimeoutMs,
              failureCode,
              stderr,
              stdout,
            }),
            stderr,
            stdout,
          })
        );
      })();
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

      const artifactValidation =
        await validateRequiredArtifacts(requiredArtifacts);

      if (artifactValidation.isReady) {
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
        void (async () => {
          const artifactValidation =
            await validateRequiredArtifacts(requiredArtifacts);

          if (artifactValidation.isReady) {
            finishWithSuccess();
            return;
          }

          finishWithError(
            new CodexCommandError({
              artifactIssues: artifactValidation.issues,
              code: "missing_artifacts",
              elapsedMs: Date.now() - startedAtMs,
              message: [
                "Codex CLI exited successfully without writing valid required artifacts.",
                ...artifactValidation.issues,
              ].join(" "),
              stderr,
              stdout,
            })
          );
        })();
        return;
      }

      const failureCode = inferProcessExitFailureCode(stderr, stdout);
      finishWithError(
        new CodexCommandError({
          code: failureCode,
          elapsedMs: Date.now() - startedAtMs,
          message:
            stderr ||
            `Codex CLI exited with code ${code ?? "unknown"} (${failureCode}).`,
          stderr,
          stdout,
        })
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

async function validateRequiredArtifacts(
  requiredArtifacts: RequiredArtifact[]
): Promise<{
  isReady: boolean;
  issues: string[];
}> {
  const artifactStatuses = await Promise.all(
    requiredArtifacts.map(async (artifact) => {
      try {
        const fileStat = await stat(artifact.filePath);

        if (!fileStat.isFile() || fileStat.size === 0) {
          return {
            isReady: false,
            issue: `${artifact.filePath} is missing or empty.`,
          };
        }

        if (!artifact.validateContents) {
          return { isReady: true, issue: null };
        }

        const contents = await readRequiredArtifact(artifact.filePath);
        artifact.validateContents(contents);
        return { isReady: true, issue: null };
      } catch (error) {
        return {
          isReady: false,
          issue:
            error instanceof Error
              ? `${artifact.filePath}: ${error.message}`
              : `${artifact.filePath}: artifact validation failed.`,
        };
      }
    })
  );

  const issues = artifactStatuses
    .map((status) => status.issue)
    .filter((issue): issue is string => issue !== null);

  return {
    isReady: issues.length === 0,
    issues,
  };
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

function inferProcessExitFailureCode(
  stderr: string,
  stdout: string
): CodexCommandFailureCode {
  return looksLikeStartupFailure(stderr, stdout)
    ? "startup_failure"
    : "process_exit";
}

function inferTimeoutFailureCode(
  stderr: string,
  stdout: string
): CodexCommandFailureCode {
  return looksLikeStartupFailure(stderr, stdout)
    ? "startup_failure"
    : "timeout";
}

function looksLikeStartupFailure(stderr: string, stdout: string): boolean {
  const text = `${stderr}\n${stdout}`.toLowerCase();
  return (
    text.includes("startup_timeout") ||
    text.includes("startup timeout") ||
    text.includes("failed to start") ||
    text.includes("mcp server") ||
    text.includes("playwright")
  );
}

function buildTimeoutMessage(options: {
  artifactIssues: string[];
  commandTimeoutMs: number;
  failureCode: CodexCommandFailureCode;
  stderr: string;
  stdout: string;
}): string {
  return [
    `Codex CLI timed out after ${options.commandTimeoutMs}ms (${options.failureCode}).`,
    options.artifactIssues.length > 0
      ? `artifact issues: ${options.artifactIssues.join(" | ")}`
      : null,
    options.stderr.trim().length > 0
      ? `stderr: ${options.stderr.trim()}`
      : null,
    options.stdout.trim().length > 0
      ? `stdout: ${options.stdout.trim()}`
      : null,
  ]
    .filter((part): part is string => part !== null)
    .join(" ");
}
