import fs from "node:fs/promises";
import os from "node:os";
import { resolveTimerTimeoutMs } from "@openclaw/normalization-core/number-coercion";
import { z } from "zod";
import { hasCommandProcessCleanupError } from "../process/exec-result.js";
import { runUtf8CommandWithTimeout } from "../process/exec.js";
import { ignoreMissingPath } from "./errno.js";
import { hasNodeErrorCode } from "./path-guards.js";
import { prepareRuntimeProcessScriptInvocation } from "./runtime-process-url.js";
import { resolveAggregateSqliteInspectionTimeoutMs } from "./sqlite-readonly-worker.js";
import { createUpdateStateInspectionDiagnostics } from "./update-candidate-state.diagnostics.js";
import { withUpdateStateInspectionWork } from "./update-candidate-state.process.js";

const inventorySchema = z.array(
  z.object({ path: z.string(), sizeBytes: z.string().regex(/^\d+$/).optional() }),
);

export async function updateStateDatabaseExists(file: string): Promise<boolean> {
  try {
    await fs.access(file);
    return true;
  } catch (error) {
    return ignoreMissingPath(error) ?? false;
  }
}

/** Maintenance custody permits metadata reads without an isolated inventory process. */
export async function readUpdateStateDatabaseSizesInProcess(
  files: readonly string[],
  signal?: AbortSignal,
): Promise<Array<{ path: string; sizeBytes: bigint | undefined }>> {
  const result: Array<{ path: string; sizeBytes: bigint | undefined }> = [];
  for (const file of files) {
    signal?.throwIfAborted();
    let sizeBytes: bigint | undefined;
    try {
      sizeBytes = (await fs.stat(file, { bigint: true })).size;
    } catch (error) {
      if (!hasNodeErrorCode(error, "ENOENT")) {
        result.push({ path: file, sizeBytes: undefined });
      }
      continue;
    }
    for (const suffix of ["-wal", "-shm", "-journal"]) {
      try {
        sizeBytes += (await fs.stat(file + suffix, { bigint: true })).size;
      } catch (error) {
        if (!hasNodeErrorCode(error, "ENOENT")) {
          sizeBytes = undefined;
          break;
        }
      }
    }
    result.push({ path: file, sizeBytes });
  }
  return result;
}

/** One child run keeps its inventory payload well under the 1 MiB stdout cap. */
const STATE_SIZES_PATHS_PER_CHUNK = 1_000;

function chunkStateSizePaths(files: readonly string[]): string[][] {
  const chunks: string[][] = [];
  for (let index = 0; index < files.length; index += STATE_SIZES_PATHS_PER_CHUNK) {
    chunks.push(files.slice(index, index + STATE_SIZES_PATHS_PER_CHUNK));
  }
  return chunks;
}

/** Measure SQLite families in bounded children so metadata cannot block updater cancellation. */
export async function readUpdateStateDatabaseSizes(
  files: readonly string[],
  options: {
    nodeRunner: string;
    sourceEnv: NodeJS.ProcessEnv;
    stagingRoot: string;
    timeoutMs?: number;
    signal?: AbortSignal;
  },
): Promise<Array<{ path: string; sizeBytes: bigint | undefined }>> {
  if (files.length === 0) {
    return [];
  }
  // Large state directories carry hundreds of thousands of paths; one child cannot
  // return that inventory without tripping the stdout cap, so measure in bounded
  // chunks and concatenate. Each chunk keeps the existing per-source budget.
  const chunks = chunkStateSizePaths(files);
  const totalBudget = resolveAggregateSqliteInspectionTimeoutMs(
    "state schema inventory",
    files.map((file) => ({ path: file, sizeBytes: undefined })),
  );
  const perChunkBudget = Math.max(1, Math.floor(totalBudget / chunks.length));
  const inspection = createUpdateStateInspectionDiagnostics({
    operation: "State schema inventory",
    phase: "metadata inventory",
    paths: files,
  });
  const measured: Array<{ path: string; sizeBytes: bigint | undefined }> = [];
  for (const chunk of chunks) {
    options.signal?.throwIfAborted();
    let result;
    let dispose: (() => void) | undefined;
    let cleanupUncertain = false;
    try {
      // Embedded source remains available after activation replaces the updater package.
      const script = prepareRuntimeProcessScriptInvocation("updateStateSizes", options.nodeRunner, {
        parentDirectory: options.stagingRoot,
      });
      dispose = () => script.dispose();
      result = await withUpdateStateInspectionWork(
        () =>
          runUtf8CommandWithTimeout(script.argv, {
            cwd: os.tmpdir(),
            input: JSON.stringify({ files: chunk }),
            baseEnv: options.sourceEnv,
            env: { XDG_CACHE_HOME: options.stagingRoot },
            signal: options.signal,
            timeoutMs: resolveTimerTimeoutMs(
              Math.max(options.timeoutMs ?? 0, perChunkBudget),
              perChunkBudget,
            ),
            killGraceMs: 500,
            killProcessTree: true,
            maxOutputBytes: { stdout: 1024 * 1024, stderr: 20_000 },
            outputCapture: { stdout: "head", stderr: "discard" },
            terminateOnOutputLimit: { stdout: true },
            onOutputChunk: inspection.onOutputChunk,
          }),
        options.signal,
      );
    } catch (error) {
      cleanupUncertain = hasCommandProcessCleanupError(error);
      if (!cleanupUncertain) {
        options.signal?.throwIfAborted();
      }
      throw inspection.failure(error);
    } finally {
      if (!cleanupUncertain) {
        dispose?.();
      }
    }
    options.signal?.throwIfAborted();
    if (result.code !== 0 || result.termination !== "exit" || result.outputLimitExceeded) {
      throw inspection.failure(
        inspection.stderr(),
        result.outputLimitExceeded ? "output-limit" : result.termination,
      );
    }
    try {
      measured.push(
        ...inventorySchema.parse(JSON.parse(result.stdout)).map((entry) => ({
          path: entry.path,
          sizeBytes: entry.sizeBytes === undefined ? undefined : BigInt(entry.sizeBytes),
        })),
      );
    } catch (error) {
      throw inspection.failure(error);
    }
  }
  return measured;
}
