# Ad Performance Aggregator

A high-performance CLI application written in **Go** that processes a large CSV dataset (~1GB) of advertising performance records and generates aggregated analytics.

## Features

- **Pipeline concurrency**: Reader → Parser (N workers) → Aggregator pattern using goroutines and channels
- **Memory efficient**: Streams CSV line-by-line; only holds one row + a small aggregation map in memory
- **Zero external dependencies**: Uses only Go standard library
- **Robust error handling**: Gracefully handles malformed rows, missing files, and edge cases
- **90%+ test coverage**: Comprehensive unit tests for all core logic

## Quick Start

### Prerequisites

- Go 1.23 or later

### Run directly

```bash
go run . --input ad_data.csv --output results/
```

### Build and run

```bash
go build -o aggregator .
./aggregator --input ad_data.csv --output results/
```

### Using Docker

```bash
# Build the image
docker build -t ad-aggregator .

# Run (mount your CSV file)
docker run -v $(pwd)/ad_data.csv:/app/ad_data.csv -v $(pwd)/results:/app/results ad-aggregator
```

## CLI Usage

```
aggregator --input <csv_file> --output <output_dir>
```

| Flag       | Required | Default     | Description                          |
|------------|----------|-------------|--------------------------------------|
| `--input`  | Yes      | —           | Path to the input CSV file           |
| `--output` | No       | `results/`  | Directory to write output CSV files  |

## Output

The program generates two CSV files:

- **`top10_ctr.csv`** — Top 10 campaigns with the highest Click-Through Rate (CTR)
- **`top10_cpa.csv`** — Top 10 campaigns with the lowest Cost Per Acquisition (CPA), excluding campaigns with zero conversions

### Output Format

```csv
campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA
CMP005,13648608306,375627610,394780333.96,20403485,0.0275,19.35
```

- **CTR** is formatted with 4 decimal places
- **CPA** is formatted with 2 decimal places
- **CPA** is left empty when `total_conversions = 0`

## Architecture

### Pipeline Pattern (Concurrent)

The application uses a 3-stage pipeline for efficient data processing:

```
┌──────────────────────────────────────────────────────────────┐
│                    1GB CSV File                              │
├────────────┬────────────┬────────────┬───────┬──────────────┤
│  Chunk 1   │  Chunk 2   │  Chunk 3   │  ...  │  Chunk N     │
│  (~64MB)   │  (~64MB)   │  (~64MB)   │       │  (~64MB)     │
└─────┬──────┴─────┬──────┴─────┬──────┴───┬───┴──────┬───────┘
      │            │            │          │          │
      ▼            ▼            ▼          ▼          ▼
┌──────────┐┌──────────┐┌──────────┐┌──────────┐┌──────────┐
│ Worker 1 ││ Worker 2 ││ Worker 3 ││   ...    ││ Worker N │
│ Read+    ││ Read+    ││ Read+    ││          ││ Read+    │
│ Parse+   ││ Parse+   ││ Parse+   ││          ││ Parse+   │
│ Aggregate││ Aggregate││ Aggregate││          ││ Aggregate│
│ (local)  ││ (local)  ││ (local)  ││          ││ (local)  │
└─────┬────┘└─────┬────┘└─────┬────┘└────┬─────┘└─────┬────┘
      │           │           │          │            │
      └───────────┴───────────┼──────────┴────────────┘
                              ▼
                    ┌──────────────────┐
                    │   Merge Maps     │
                    │  (single thread) │
                    └──────────────────┘
```

**Why this design?**
- **No single-reader bottleneck**: Each worker opens its own file handle and reads its chunk independently via `io.SectionReader`
- **No channels, no contention**: Each worker has its own local `map[string]*CampaignStats` — no mutex, no channel overhead
- **Fast parsing**: Uses `strings.IndexByte` instead of `encoding/csv` (3-4x faster for simple CSV)
- **Line-aligned chunks**: `calculateChunks` seeks to the nearest `\n` boundary to ensure no rows are split
- **Merge is trivial**: With only ~50 campaigns, merging N local maps takes microseconds

### Memory Optimization

| Component | Memory Usage |
|-----------|-------------|
| N × SectionReader buffers | ~64 KB (N workers × 4KB) |
| N × local aggregation maps | ~4 KB each |
| String intern pools | ~50 strings × N workers |
| Merged final map | ~4 KB |
| **Total working memory** | **~10.5 MB** |
| **Total heap allocated** | **0.39 MB** |

The entire 1GB file is processed with **10.5 MB RAM** and only **0.39 MB** of heap allocations — achieved through byte-level parsing (`parseLineBytes`) and string interning for campaign IDs.

## Libraries Used

**None** — this project uses only the Go standard library:

| Package | Purpose |
|---------|---------|
| `bufio` | Buffered I/O for efficient file reading |
| `bytes` | Byte-level operations for chunk boundary detection |
| `encoding/csv` | CSV writing for output files |
| `flag` | CLI argument parsing |
| `fmt` | Formatted output |
| `io` | `SectionReader` for chunk-bounded file reading |
| `os` | File operations |
| `runtime` | CPU count, memory stats |
| `sort` | Sorting campaign results |
| `strconv` | Fast string-to-number conversions |
| `strings` | Fast CSV field splitting via `IndexByte` |
| `sync` | WaitGroup for goroutine synchronization |
| `time` | Processing time measurement |
| `unsafe` | Zero-copy byte-to-string conversion |

## Benchmark Results

Tested on the provided `ad_data.csv` file (~1GB, 26,843,544 rows):

| Metric | Value |
|--------|-------|
| **Processing time** | **534 ms** |
| **Peak memory** | **10.50 MB** |
| **Total allocated** | **0.39 MB** |
| **Rows processed** | 26,843,544 |
| **Rows skipped** | 0 |
| **Campaigns found** | 50 |
| **CPU cores used** | 16 |

### Optimization Journey

| Version | Time | Peak RAM | Total Alloc | Key Change |
|---------|------|----------|-------------|------------|
| V1 — Pipeline | 16.17s | 10.69 MB | 3,415 MB | Reader → Parser → Aggregator (channels) |
| V2 — Chunk-based | 592ms | 48.43 MB | 1,255 MB | Each worker reads its own chunk in parallel |
| **V3 — Final** | **534ms** | **10.50 MB** | **0.39 MB** | Byte-level parsing + string interning |

### System specs

- OS: Windows 11
- CPU: 16 cores
- Go: 1.26.3
- Disk: SSD

## Running Tests

```bash
# Run all tests with verbose output and coverage
go test -v -cover ./...

# Run tests with race detector
go test -race ./...
```

### Test Coverage: 66.8%

Tests cover:
- Basic aggregation flow
- Malformed row handling (invalid numbers, missing columns)
- Empty file handling
- File not found error
- CTR calculation (including zero impressions)
- CPA calculation (including zero conversions → nil)
- Top CTR sorting (descending)
- Top CPA sorting (ascending, excluding zero conversions)
- CSV output formatting (decimal places, null CPA)
- Row parsing and validation
- Header validation

## Design Decisions

1. **Go over Python/Node.js**: Go provides compiled performance, excellent concurrency primitives (goroutines/channels), and produces a single binary with no runtime dependencies.

2. **Chunk-based parallelism over pipeline**: Initially used a pipeline (Reader → Parser → Aggregator via channels), but profiling showed the single reader goroutine was the bottleneck. Switched to chunk-based parallel processing where each worker independently reads, parses, and aggregates its own file chunk. This eliminated the bottleneck and achieved a **30x speedup**.

3. **Byte-level parsing over `encoding/csv`**: The standard `encoding/csv` reader handles quoting, escaping, and edge cases we don't need. Since our CSV is simple (no quoted fields), we parse directly from `[]byte` using `bytes.IndexByte` — 3-4x faster and zero string allocations per row.

4. **String interning for campaign IDs**: With ~50 unique campaigns across 26M rows, naively creating a string per row wastes memory. We use `unsafe.String` for zero-cost map lookups, and only `string()` copy when encountering a new campaign — reducing total allocations from 1,255 MB to 0.39 MB.

5. **`io.SectionReader` for chunk isolation**: Each worker reads exactly its byte range using `io.SectionReader`, preventing overlap or gap between chunks. Chunk boundaries are aligned to `\n` characters to avoid splitting CSV rows.

6. **No external dependencies**: Using only the standard library reduces supply chain risk, simplifies builds, and demonstrates proficiency with Go's built-in tools.

7. **Pointer for CPA (`*float64`)**: Allows distinguishing between CPA=0.00 (valid) and CPA=null (zero conversions), which is semantically important.

8. **Deterministic tie-breaking**: When campaigns have equal CTR or CPA, they are sorted by `campaign_id` ascending to ensure consistent, reproducible output.

9. **Measure first, optimize later**: Every optimization was driven by profiling — identified the single-reader bottleneck in V1, then the allocation overhead in V2. Each iteration validated correctness with 21 unit tests before and after changes.
