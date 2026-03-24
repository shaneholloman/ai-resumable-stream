/**
 * Memory Profile: TEE vs DRAIN over HTTP
 *
 * Run: pnpm tsx scripts/memory-profile.ts
 * Run with mitata: pnpm tsx --expose-gc scripts/memory-profile.ts --mitata
 */
import http from "node:http";
import os from "node:os";
import type { UIMessageChunk } from "ai";
import { createClient } from "redis";
import { RedisMemoryServer } from "redis-memory-server";
import { createResumableUIMessageStream as createTee } from "../src/resumable-ui-message-stream.main.js";
import { createResumableUIMessageStream as createDrain } from "../src/resumable-ui-message-stream.js";

type Redis = ReturnType<typeof createClient>;

const TOTAL_CHUNKS = 100;
const CHUNK_SIZE = 1_000;
const WARMUP = 3;
const ITERATIONS = 10;

const useMitata = process.argv.includes("--mitata");

let redisServer: RedisMemoryServer;
let redisUrl: string;
let publisher: Redis;
let subscriber: Redis;
let server: http.Server;
let currentCreateContext: typeof createTee;

async function setupRedis(): Promise<void> {
  redisServer = new RedisMemoryServer();
  const host = await redisServer.getHost();
  const port = await redisServer.getPort();
  redisUrl = `redis://${host}:${port}`;

  publisher = createClient({ url: redisUrl });
  subscriber = createClient({ url: redisUrl });
  await Promise.all([publisher.connect(), subscriber.connect()]);
}

async function setupServer(port: number): Promise<void> {
  server = http.createServer(async (_, res) => {
    const pendingPromises: Array<Promise<unknown>> = [];
    const waitUntil = (p: Promise<unknown>) => pendingPromises.push(p);

    const context = await currentCreateContext({
      streamId: `stream-${Date.now()}-${Math.random()}`,
      publisher,
      subscriber,
      waitUntil,
    });

    const source = createSource();
    const clientStream = await context.startStream(source);
    const iterator = clientStream[Symbol.asyncIterator]();

    res.writeHead(200, { "Content-Type": "application/x-ndjson" });

    try {
      while (true) {
        const { done, value } = await iterator.next();
        if (done) break;
        res.write(JSON.stringify(value) + "\n");
      }
    } finally {
      res.end();
    }
  });

  await new Promise<void>((r) => server.listen(port, r));
}

function createSource(): ReadableStream<UIMessageChunk> {
  const data = "x".repeat(CHUNK_SIZE);
  return new ReadableStream({
    async start(controller) {
      for (let i = 0; i < TOTAL_CHUNKS; i++) {
        controller.enqueue({ type: "text-delta", id: `${i}`, delta: data });
        await new Promise((r) => setTimeout(r, 5));
      }
      controller.close();
    },
  });
}

async function runScenario(): Promise<{ timeMs: number; heapBytes: number }> {
  const heapBefore = process.memoryUsage().heapUsed;
  const timeBefore = performance.now();

  const res = await fetch(`http://localhost:3456`);
  const reader = res.body!.getReader();
  while (true) {
    const { done } = await reader.read();
    if (done) break;
  }

  const timeMs = performance.now() - timeBefore;
  const heapBytes = process.memoryUsage().heapUsed - heapBefore;

  return { timeMs, heapBytes: Math.max(0, heapBytes) };
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes.toFixed(0)} b`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} kb`;
  return `${(bytes / 1024 / 1024).toFixed(2)} mb`;
}

function formatMs(ms: number): string {
  return `${ms.toFixed(2)} ms`;
}

interface Stats {
  min: number;
  max: number;
  avg: number;
  p75: number;
  p99: number;
}

function getStats(samples: Array<number>): Stats {
  const sorted = [...samples].sort((a, b) => a - b);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  const avg = sorted.reduce((a, b) => a + b, 0) / sorted.length;
  const p75 = sorted[Math.floor(sorted.length * 0.75)];
  const p99 = sorted[Math.floor(sorted.length * 0.99)];
  return { min, max, avg, p75, p99 };
}

interface BenchResult {
  name: string;
  time: Stats;
  heap: Stats;
}

async function runBenchmark(name: string, createContext: typeof createTee): Promise<BenchResult> {
  currentCreateContext = createContext;

  /** Warmup iterations */
  for (let i = 0; i < WARMUP; i++) {
    if (global.gc) global.gc();
    await runScenario();
  }

  /** Measured iterations */
  const timeSamples: Array<number> = [];
  const heapSamples: Array<number> = [];

  for (let i = 0; i < ITERATIONS; i++) {
    if (global.gc) global.gc();
    const { timeMs, heapBytes } = await runScenario();
    timeSamples.push(timeMs);
    heapSamples.push(heapBytes);
  }

  return {
    name,
    time: getStats(timeSamples),
    heap: getStats(heapSamples),
  };
}

function printResults(results: Array<BenchResult>): void {
  const dim = (s: string) => `\x1b[90m${s}\x1b[0m`;
  const bold = (s: string) => `\x1b[1m${s}\x1b[0m`;
  const cyan = (s: string) => `\x1b[36m${s}\x1b[0m`;
  const green = (s: string) => `\x1b[32m${s}\x1b[0m`;
  const yellow = (s: string) => `\x1b[33m${s}\x1b[0m`;

  console.log(dim(`cpu: ${os.cpus()[0].model}`));
  console.log(dim(`runtime: node ${process.version} (${process.arch}-${process.platform})`));
  console.log();

  console.log(`${"benchmark".padEnd(28)} ${"time".padEnd(30)} ${"heap"}`);
  console.log(`${"-".repeat(28)} ${"-".repeat(30)} ${"-".repeat(30)}`);

  for (const r of results) {
    const timeStr = `${yellow(formatMs(r.time.avg))} ${dim(`(${formatMs(r.time.min)} … ${formatMs(r.time.max)})`)}`;
    const heapStr = `${yellow(formatBytes(r.heap.avg))} ${dim(`(${formatBytes(r.heap.min)} … ${formatBytes(r.heap.max)})`)}`;
    console.log(`${r.name.padEnd(28)} ${timeStr.padEnd(50)} ${heapStr}`);
  }

  console.log();
  console.log(bold("summary"));

  const [first, second] = results;
  const timeRatio = first.time.avg / second.time.avg;
  const heapRatio = first.heap.avg / second.heap.avg;

  const faster = timeRatio > 1 ? second : first;
  const slower = timeRatio > 1 ? first : second;
  const speedup = Math.max(timeRatio, 1 / timeRatio);

  console.log(`  ${bold(cyan(faster.name))}`);
  console.log(`   ${green(speedup.toFixed(2) + "x")} faster than ${bold(cyan(slower.name))}`);

  const lessHeap = heapRatio > 1 ? second : first;
  const moreHeap = heapRatio > 1 ? first : second;
  const heapDiff = ((moreHeap.heap.avg - lessHeap.heap.avg) / moreHeap.heap.avg) * 100;

  console.log();
  console.log(bold("memory"));
  console.log(`  ${bold(cyan(lessHeap.name))}`);
  console.log(
    `   ${green(heapDiff.toFixed(0) + "%")} less heap usage than ${bold(cyan(moreHeap.name))}`,
  );
}

async function runWithMitata(): Promise<void> {
  const { bench, boxplot, run, summary } = await import("mitata");

  summary(() => {
    boxplot(() => {
      bench("OLD (tee + pipeThrough)", async () => {
        currentCreateContext = createTee;
        await runScenario();
      }).gc("inner");

      bench("NEW (single drain loop)", async () => {
        currentCreateContext = createDrain;
        await runScenario();
      }).gc("inner");
    });
  });

  await run();
}

async function main() {
  process.on("unhandledRejection", () => {});

  console.log(`Scenario: HTTP client consumes full stream`);
  console.log(`  Chunks: ${TOTAL_CHUNKS} × ${CHUNK_SIZE} bytes`);
  console.log(`  Warmup: ${WARMUP} iterations`);
  console.log(`  Iterations: ${ITERATIONS}`);
  console.log();

  await setupRedis();
  await setupServer(3456);

  if (useMitata) {
    await runWithMitata();
  } else {
    const tee = await runBenchmark("OLD (tee + pipeThrough)", createTee);
    const drain = await runBenchmark("NEW (single drain loop)", createDrain);
    printResults([tee, drain]);
  }

  server.close();
  await Promise.all([publisher.quit(), subscriber.quit()]);
  await redisServer.stop();
}

main();
