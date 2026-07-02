# PROMPTS.md — AI Coding Assistants Log

Below are the raw prompts I used while working on this challenge, in chronological order.

---

## Phase 1: Understanding the Problem

### Prompt 1: Đọc và phân tích đề bài
```
Đọc kĩ readme.md
```
> Mục đích: Yêu cầu AI đọc toàn bộ file README.md của đề bài để hiểu rõ yêu cầu. Sau đó AI phân tích chi tiết từng phần: input format, output format, các chỉ số cần tính (CTR, CPA), edge cases cần xử lý.

### Prompt 2: Phân tích yêu cầu chi tiết
```
Phân tích cho t bằng tiếng việt, nó yêu cầu những gì
```
> Mục đích: Yêu cầu AI tổng hợp lại toàn bộ yêu cầu bằng tiếng Việt để hiểu chính xác từng điểm cần triển khai, bao gồm:
> - 6 cột CSV đầu vào
> - Công thức CTR và CPA
> - 2 file output CSV (top10_ctr.csv, top10_cpa.csv)
> - Yêu cầu kỹ thuật (hiệu năng, CLI, tests, documentation)

---

## Phase 2: Planning & Architecture Design

### Prompt 3: Lên kế hoạch triển khai
```
Lên planing cho cách giải quyết bài toán này
```
> Mục đích: Yêu cầu AI xây dựng implementation plan chi tiết bao gồm:
> - Lý do chọn Go (bảng so sánh Go vs Python vs Node.js)
> - Kiến trúc pipeline: Reader → Parser → Aggregator
> - Chiến lược tối ưu bộ nhớ (streaming, HashMap aggregation)
> - Cấu trúc project (các file cần tạo)
> - Edge cases cần xử lý
> - Verification plan

### Prompt 4: Tìm hiểu về concurrency
```
Làm sao để thực hiện concurrency được
```
> Mục đích: Trước khi quyết định kiến trúc, tôi cần hiểu rõ các cách tiếp cận concurrency cho bài toán đọc CSV:
> - Pipeline Pattern (Producer-Consumer)
> - Chunk-based Parallelism
> - Single-threaded Streaming
> 
> AI phân tích ưu/nhược từng cách, giúp tôi đưa ra quyết định đúng.

### Prompt 5: Chọn kiến trúc
```
thực thi theo Pipeline Pattern và concurrency đi
```
> Quyết định: Chọn Pipeline Pattern vì:
> - Thể hiện kỹ năng Go nâng cao (goroutine, channel, sync)
> - Tách biệt rõ ràng I/O-bound (reader) vs CPU-bound (parser)
> - Code sạch và dễ hiểu

---

## Phase 3: Implementation

> Sau khi plan được approve, AI triển khai code theo thứ tự:
> 1. `go.mod` — Module init (chỉ dùng standard library)
> 2. `aggregator/types.go` — Struct definitions
> 3. `aggregator/aggregator.go` — Core pipeline logic
> 4. `aggregator/writer.go` — CSV output writer
> 5. `main.go` — CLI entry point + benchmark
> 6. `aggregator/aggregator_test.go` — 21 unit tests
> 7. `Dockerfile` — Multi-stage build

### Kết quả lần chạy đầu tiên (Pipeline Pattern):
```
Processing time:  16.172s
Peak memory:      10.69 MB
Rows processed:   26,843,544
Tests:            21/21 PASS (90.2% coverage)
```

---

## Phase 4: Performance Optimization

### Prompt 6: Tối ưu tốc độ
```
Đang chạy 16s, có cách nào nhanh hơn không? Thử chunk-based parallel — mỗi worker đọc 1 phần file riêng
```
> Kết quả: Chuyển sang chunk-based parallel + bỏ `encoding/csv` dùng `bytes.IndexByte` → **592ms** (27x nhanh hơn). Peak memory tăng từ 10MB lên 48MB.

### Prompt 7: Tối ưu bộ nhớ
```
Tốc độ ok rồi nhưng RAM tăng lên 48MB, total alloc 1,255MB. Giảm được không?
```
> Kết quả: Giảm scanner buffer 1MB→4KB, parse từ `[]byte` trực tiếp, string interning cho campaign IDs → peak memory về **10.5MB**, total alloc còn **0.39MB**.

### Kết quả cuối cùng:
```
Processing time:  534ms (30x nhanh hơn baseline)
Peak memory:      10.50 MB (giữ nguyên so với baseline)
Total allocated:  0.39 MB (giảm 8,756x so với V2!)
```

---

## Tổng kết quá trình tối ưu

| Version | Thời gian | Peak RAM | Total Alloc | Kiến trúc |
|---------|-----------|----------|-------------|-----------|
| V1 — Pipeline | 16.17s | 10.69 MB | 3,415 MB | Reader → Parser → Aggregator (channels) |
| V2 — Chunk-based | 592ms | 48.43 MB | 1,255 MB | N workers, mỗi worker tự đọc chunk |
| V3 — Memory-opt | 534ms | 10.50 MB | 0.39 MB | + byte parsing + string interning |

### Tư duy tối ưu:
1. **Đo trước, tối ưu sau**: Luôn có baseline trước khi tối ưu
2. **Xác định bottleneck**: V1 bottleneck ở single reader → V2 giải quyết bằng chunk-based parallel
3. **Trade-off có ý thức**: V2 nhanh hơn 27x nhưng RAM tăng 5x → V3 áp dụng zero-alloc techniques để giảm RAM lại
4. **Correctness first**: Mỗi lần tối ưu đều chạy lại 21 unit tests để đảm bảo kết quả không thay đổi
