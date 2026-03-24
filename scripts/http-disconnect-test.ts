/**
 * HTTP Disconnect Test
 *
 * Tests whether client disconnect (response going out of scope) triggers
 * the cancel() callback on the server-side stream.
 *
 * Run: pnpm tsx scripts/http-disconnect-test.ts
 */
import http from "node:http";
import type { UIMessageChunk } from "ai";
import { createClient } from "redis";
import { RedisMemoryServer } from "redis-memory-server";
import { createResumableUIMessageStream as createTee } from "../src/resumable-ui-message-stream.main.js";
import { createResumableUIMessageStream as createDrain } from "../src/resumable-ui-message-stream.js";

type Redis = ReturnType<typeof createClient>;

const CHUNKS_PER_STREAM = 100;
const CHUNK_SIZE_BYTES = 1_000;
const SERVER_PORT = 3456;

let redisServer: RedisMemoryServer;
let redisUrl: string;

async function setupRedis(): Promise<void> {
  console.log(`Starting Redis memory server...`);
  redisServer = new RedisMemoryServer();
  const host = await redisServer.getHost();
  const port = await redisServer.getPort();
  redisUrl = `redis://${host}:${port}`;
  console.log(`Redis ready at ${redisUrl}`);
}

async function teardownRedis(): Promise<void> {
  await redisServer.stop();
}

function createRedisClient(): Redis {
  return createClient({ url: redisUrl });
}

/**
 * Creates a slow push-based stream to give client time to disconnect
 */
function createSlowStream(chunkCount: number, label: string): ReadableStream<UIMessageChunk> {
  const textContent = "x".repeat(CHUNK_SIZE_BYTES);

  return new ReadableStream<UIMessageChunk>({
    async start(controller) {
      console.log(`  [${label}] Source stream starting...`);
      for (let i = 0; i < chunkCount; i++) {
        controller.enqueue({
          id: `chunk-${i}`,
          type: "text-delta",
          delta: `${textContent}-${i}`,
        });
        /** Slow down to allow disconnect to happen mid-stream */
        await new Promise((r) => setTimeout(r, 10));
      }
      controller.close();
      console.log(`  [${label}] Source stream completed (${chunkCount} chunks)`);
    },
    cancel(reason) {
      console.log(`  [${label}] Source stream CANCELLED:`, reason);
    },
  });
}

interface TestResult {
  implementation: string;
  cancelCalled: boolean;
  chunksBeforeCancel: number;
  totalChunks: number;
  memoryGrowthMB: number;
  redisChunksReceived: number;
}

async function testImplementation(
  name: string,
  createContextFn: typeof createTee,
): Promise<TestResult> {
  if (global.gc) global.gc();
  await new Promise((r) => setTimeout(r, 100));

  const startMemory = process.memoryUsage().heapUsed / 1024 / 1024;

  let cancelCalled = false;
  let chunksEnqueued = 0;
  let streamId = "";
  let resolveServerDone: () => void;
  const serverDone = new Promise<void>((r) => {
    resolveServerDone = r;
  });

  /** Track background promises to wait for them before cleanup */
  const pendingPromises: Array<Promise<unknown>> = [];
  const waitUntil = (p: Promise<unknown>) => pendingPromises.push(p);

  const publisher = createRedisClient();
  const subscriber = createRedisClient();
  await Promise.all([publisher.connect(), subscriber.connect()]);

  const server = http.createServer(async (req, res) => {
    streamId = `test-${Date.now()}`;

    const context = await createContextFn({
      streamId,
      publisher,
      subscriber,
      waitUntil,
    });

    /**
     * Create a stream that tracks chunks and wraps cancel detection
     */
    const sourceStream = createSlowStream(CHUNKS_PER_STREAM, name);

    const clientStream = await context.startStream(sourceStream);

    /**
     * Get a reader from the async iterable's underlying stream
     * so we can explicitly cancel it when client disconnects
     */
    const clientReader = clientStream[Symbol.asyncIterator]();

    /**
     * Convert async iterable to web stream for HTTP response
     */
    const responseStream = new ReadableStream({
      async start(controller) {
        try {
          while (true) {
            const { done, value } = await clientReader.next();
            if (done) break;
            chunksEnqueued++;
            const data = JSON.stringify(value) + "\n";
            controller.enqueue(new TextEncoder().encode(data));
          }
          controller.close();
        } catch (err) {
          controller.error(err);
        }
      },
      async cancel() {
        cancelCalled = true;
        console.log(`  [${name}] responseStream.cancel() after ${chunksEnqueued} chunks`);

        /** Propagate cancel to the actual client stream from TEE/DRAIN */
        if (clientReader.return) {
          await clientReader.return();
          console.log(`  [${name}] clientStream iterator returned (cancelled)`);
        }
      },
    });

    res.writeHead(200, {
      "Content-Type": "application/x-ndjson",
      "Transfer-Encoding": "chunked",
    });

    /**
     * Detect client disconnect via 'close' event
     */
    let clientDisconnected = false;
    res.on("close", () => {
      if (!res.writableEnded) {
        clientDisconnected = true;
        console.log(`  [${name}] Client disconnected (res 'close' event)`);
      }
    });

    /**
     * Pipe the stream to the response
     */
    const reader = responseStream.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        /** Check if client disconnected before writing */
        if (clientDisconnected) {
          console.log(`  [${name}] Stopping write loop - client disconnected`);
          await reader.cancel();
          break;
        }

        const canContinue = res.write(value);
        if (!canContinue) {
          /** Wait for drain if buffer is full */
          await new Promise((r) => res.once("drain", r));
        }
      }
      if (!clientDisconnected) {
        res.end();
      }
    } catch (err) {
      console.log(`  [${name}] Write error:`, err);
    } finally {
      reader.releaseLock();
      resolveServerDone();
    }
  });

  await new Promise<void>((resolve) => {
    server.listen(SERVER_PORT, resolve);
  });

  console.log(`\n=== ${name} ===`);
  console.log(`  Server listening on port ${SERVER_PORT}`);

  /**
   * Client: fetch but abort connection mid-stream
   */
  console.log(`  Client: fetching and will abort mid-stream...`);

  const abortController = new AbortController();

  const fetchPromise = (async () => {
    try {
      const response = await fetch(`http://localhost:${SERVER_PORT}/stream`, {
        signal: abortController.signal,
      });
      console.log(`  Client: got response, status ${response.status}`);

      /** Read just a tiny bit to start the stream */
      const reader = response.body?.getReader();
      if (reader) {
        await reader.read();
        console.log(`  Client: read first chunk, now aborting connection...`);
      }
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        console.log(`  Client: fetch aborted as expected`);
      } else {
        throw err;
      }
    }
  })();

  /** Give server time to start streaming, then abort */
  await new Promise((r) => setTimeout(r, 50));
  abortController.abort();

  await fetchPromise;

  /** Wait for server to detect the disconnect */
  console.log(`  Waiting for server to process disconnect...`);
  await Promise.race([serverDone, new Promise((r) => setTimeout(r, 3_000))]);

  /** Wait for source stream to complete (100 chunks * 10ms = 1s) */
  console.log(`  Waiting for source stream to complete...`);
  await new Promise((r) => setTimeout(r, 1_500));

  /** Wait for background Redis operations to complete */
  console.log(`  Waiting for Redis persistence...`);
  await Promise.race([
    Promise.allSettled(pendingPromises),
    new Promise((r) => setTimeout(r, 3_000)),
  ]);

  /** Give time for any async cleanup */
  await new Promise((r) => setTimeout(r, 500));

  /** Check how many chunks Redis received */
  let redisChunksReceived = 0;
  try {
    /** List all keys to see what's stored */
    const allKeys = await publisher.keys("*");
    console.log(`  All Redis keys:`);
    for (const key of allKeys) {
      const type = await publisher.type(key);
      if (type === "stream") {
        const len = await publisher.xLen(key);
        console.log(`    ${key} (${type}) - ${len} entries`);
        if (key.includes(streamId)) {
          redisChunksReceived = len;
        }
      } else {
        const val = await publisher.get(key);
        console.log(`    ${key} (${type}) = ${val}`);
      }
    }
  } catch (err) {
    console.log(`  Could not read Redis:`, err);
  }

  const finalMemory = process.memoryUsage().heapUsed / 1024 / 1024;
  const memoryGrowth = finalMemory - startMemory;

  server.close();
  await Promise.all([
    publisher.isOpen ? publisher.quit() : Promise.resolve(),
    subscriber.isOpen ? subscriber.quit() : Promise.resolve(),
  ]);

  console.log(`  Results:`);
  console.log(`    cancel() called: ${cancelCalled}`);
  console.log(`    Chunks sent to client: ${chunksEnqueued}`);
  console.log(`    Chunks in Redis: ${redisChunksReceived}`);
  console.log(`    Memory growth: ${memoryGrowth.toFixed(2)} MB`);

  return {
    implementation: name,
    cancelCalled,
    chunksBeforeCancel: chunksEnqueued,
    totalChunks: CHUNKS_PER_STREAM,
    memoryGrowthMB: memoryGrowth,
    redisChunksReceived,
  };
}

async function testNormalFlow(name: string, createContextFn: typeof createTee): Promise<void> {
  console.log(`\n=== ${name} (normal flow - no disconnect) ===`);

  const pendingPromises: Array<Promise<unknown>> = [];
  const waitUntil = (p: Promise<unknown>) => pendingPromises.push(p);

  const publisher = createRedisClient();
  const subscriber = createRedisClient();
  await Promise.all([publisher.connect(), subscriber.connect()]);

  const streamId = `normal-${Date.now()}`;

  const context = await createContextFn({
    streamId,
    publisher,
    subscriber,
    waitUntil,
  });

  /** Simple 10-chunk stream */
  const sourceStream = new ReadableStream<UIMessageChunk>({
    async start(controller) {
      for (let i = 0; i < 10; i++) {
        controller.enqueue({ type: "text-delta", id: `${i}`, delta: `chunk-${i}` });
      }
      controller.close();
      console.log(`  Source: produced 10 chunks`);
    },
  });

  const clientStream = await context.startStream(sourceStream);

  /** Consume all chunks */
  let count = 0;
  for await (const _ of clientStream) {
    count++;
  }
  console.log(`  Client: consumed ${count} chunks`);

  /** Wait for Redis */
  await Promise.allSettled(pendingPromises);
  await new Promise((r) => setTimeout(r, 500));

  /** Check Redis */
  const allKeys = await publisher.keys("*");
  console.log(`  Redis keys after normal flow:`);
  for (const key of allKeys) {
    const type = await publisher.type(key);
    if (type === "stream") {
      const len = await publisher.xLen(key);
      console.log(`    ${key} (${type}) - ${len} entries`);
    } else {
      const val = await publisher.get(key);
      console.log(`    ${key} (${type}) = ${val}`);
    }
  }

  await Promise.all([publisher.quit(), subscriber.quit()]);
}

async function main() {
  console.log(`HTTP Disconnect Test: TEE vs DRAIN`);
  console.log(`${"=".repeat(60)}`);
  console.log(`\nThis tests whether HTTP client disconnect triggers cancel().`);
  console.log(`Each implementation streams ${CHUNKS_PER_STREAM} chunks.`);
  console.log(`Client reads 1 chunk then disconnects.`);

  /** Suppress expected Redis errors during disconnect */
  process.on("unhandledRejection", (err: Error) => {
    if (err?.message?.includes("client is closed")) return;
    console.error("Unhandled rejection:", err);
  });

  await setupRedis();

  try {
    /** First, test normal flow to verify Redis persistence works */
    console.log(`\n${"=".repeat(60)}`);
    console.log(`BASELINE: Testing normal flow (no disconnect)`);
    console.log(`${"=".repeat(60)}`);
    await testNormalFlow("TEE", createTee);
    await testNormalFlow("DRAIN", createDrain);

    console.log(`\n${"=".repeat(60)}`);
    console.log(`DISCONNECT TEST`);
    console.log(`${"=".repeat(60)}`);

    const teeResult = await testImplementation("TEE", createTee);

    await new Promise((r) => setTimeout(r, 1_000));
    if (global.gc) global.gc();

    const drainResult = await testImplementation("DRAIN", createDrain);

    console.log(`\n${"=".repeat(60)}`);
    console.log(`SUMMARY`);
    console.log(`${"=".repeat(60)}`);
    console.log(`\n| Impl | cancel() | Client | Redis | Memory |`);
    console.log(`|------|----------|--------|-------|--------|`);
    for (const r of [teeResult, drainResult]) {
      console.log(
        `| ${r.implementation.padEnd(5)} | ${String(r.cancelCalled).padEnd(8)} | ${String(r.chunksBeforeCancel).padStart(6)} | ${String(r.redisChunksReceived).padStart(5)} | ${r.memoryGrowthMB.toFixed(2).padStart(6)} MB |`,
      );
    }

    console.log(`\nConclusion:`);
    if (teeResult.cancelCalled && drainResult.cancelCalled) {
      console.log(`  ✓ Both implementations receive cancel() on HTTP disconnect`);
    } else {
      console.log(
        `  ✗ cancel() was NOT called for: ${[!teeResult.cancelCalled && "TEE", !drainResult.cancelCalled && "DRAIN"].filter(Boolean).join(", ")}`,
      );
    }

    console.log(`\nRedis persistence:`);
    const allChunks = CHUNKS_PER_STREAM;
    console.log(
      `  TEE:   ${teeResult.redisChunksReceived}/${allChunks} chunks (${teeResult.redisChunksReceived === allChunks ? "✓ complete" : "✗ incomplete"})`,
    );
    console.log(
      `  DRAIN: ${drainResult.redisChunksReceived}/${allChunks} chunks (${drainResult.redisChunksReceived === allChunks ? "✓ complete" : "✗ incomplete"})`,
    );

    console.log(`\nMemory comparison:`);
    const memDiff = teeResult.memoryGrowthMB - drainResult.memoryGrowthMB;
    if (memDiff > 0) {
      console.log(`  DRAIN uses ${memDiff.toFixed(2)} MB less memory than TEE`);
    } else {
      console.log(`  TEE uses ${(-memDiff).toFixed(2)} MB less memory than DRAIN`);
    }
  } finally {
    await teardownRedis();
  }
}

main().catch(console.error);
