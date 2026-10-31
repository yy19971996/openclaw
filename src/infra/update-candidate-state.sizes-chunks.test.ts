// Large state directories carry hundreds of thousands of paths. One child cannot
// return that metadata inventory without tripping the 1 MiB stdout cap, so the
// updater measures it in bounded chunks and still seals the capture.
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import type { SpawnResult } from "../process/exec-result.js";
import * as commands from "../process/exec.js";
import { readUpdateStateDatabaseSizes } from "./update-candidate-state.sizes.js";

afterEach(() => {
  vi.restoreAllMocks();
});

it("measures a large metadata inventory in bounded chunks", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "state-sizes-chunks-"));
  const files = Array.from({ length: 2_500 }, (_, index) => path.join(root, `db-${index}.sqlite`));
  const batches: number[] = [];
  vi.spyOn(commands, "runUtf8CommandWithTimeout").mockImplementation(async (_argv, options) => {
    if (typeof options === "number") {
      throw new Error("expected command options");
    }
    const input = JSON.parse(String(options.input)) as { files: string[] };
    batches.push(input.files.length);
    return {
      code: 0,
      signal: null,
      killed: false,
      termination: "exit",
      stdout: JSON.stringify(input.files.map((file) => ({ path: file, sizeBytes: "4096" }))),
      stderr: "",
    } as SpawnResult;
  });

  const sizes = await readUpdateStateDatabaseSizes(files, {
    nodeRunner: process.execPath,
    sourceEnv: {},
    stagingRoot: root,
  });

  expect(sizes).toHaveLength(2_500);
  expect(batches).toEqual([1_000, 1_000, 500]);
  await fs.rm(root, { recursive: true, force: true });
});
