# Arrow LZ4 decoder benchmark

Requires Maven and JDK 17+. Run from the repository root:

```bash
# Build the shaded driver and verify 15 size/compressibility fixtures, dependency
# relocation, Java backend selection, and the absence of native libraries/loading.
bash tests/benchmarks/arrow-lz4/run.sh verify

# Compare direct ArrowBuf decoding, Arrow's existing codec, and Commons streaming.
# One fresh JVM per row; 400 ms warmup, 2 s measured, 1 GiB heap, single thread.
# Each codec handles either 1 or 16 buffers before being replaced.
bash tests/benchmarks/arrow-lz4/run.sh matrix

SIZE=65536 MEASURE_SECONDS=5 HEAP=512m bash tests/benchmarks/arrow-lz4/run.sh matrix

# One variant: direct|arrow|commons-stream, output bytes, pattern, buffers/codec, seconds.
bash tests/benchmarks/arrow-lz4/run.sh direct 1048576 mixed 16 5
```

CSV columns report decoded MiB/s, allocated **heap** bytes per buffer, and GC count/time.
Off-heap input buffers are reused read-only; outputs are allocated and released per decode.
All backends are Java-only. Patterns are `repeated`, `mixed` (half random), and `random`.
Fixtures use Commons' frame writer with 64 KiB / 256 KiB / 4 MiB blocks and no checksums;
they are synthetic, not real Databend pages or lz4_flex output. Correctness tests separately
cover checksums, frame validation, malformed input, and nonzero off-heap output.

This is an exploratory decoder microbenchmark, not a CI timing test, a query-speedup claim,
or an OOM guarantee. Repeat with longer measurements and relevant data before drawing
performance conclusions. Results and implementation tradeoffs belong in the PR description.
