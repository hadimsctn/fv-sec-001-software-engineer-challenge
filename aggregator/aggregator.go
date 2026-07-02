package aggregator

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

const (
	// scannerBufSize is the buffer size for bufio.Scanner.
	// CSV lines are typically ~40-60 bytes, so 4KB is more than enough.
	scannerBufSize = 4 * 1024

	// expectedColumns is the number of columns expected in each CSV row.
	expectedColumns = 6
)

// AggregateResult holds the final output of the aggregation pipeline.
type AggregateResult struct {
	Stats       map[string]*CampaignStats
	TotalRows   int64
	SkippedRows int64
	Err         error
}

// workerResult holds the local aggregation result from a single worker.
type workerResult struct {
	stats   map[string]*CampaignStats
	rows    int64
	skipped int64
}

// RunPipeline executes the optimized chunk-based parallel aggregation:
//
//  1. Read the CSV header and validate it
//  2. Divide the remaining file into N chunks (aligned to line boundaries)
//  3. Each worker goroutine processes its chunk independently with a local map
//  4. Merge all local maps into the final result
//
// This eliminates the single-reader bottleneck of the previous pipeline approach.
// Each worker does its own I/O + parsing + aggregation, maximizing throughput.
func RunPipeline(inputPath string) (*AggregateResult, error) {
	file, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	// Read and validate header
	headerReader := bufio.NewReaderSize(file, 4096)
	headerLine, err := headerReader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}
	if err := validateHeader(headerLine); err != nil {
		return nil, err
	}

	// Get file size
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Calculate the data start offset (after header)
	// We need to figure out how many bytes the header consumed.
	// The headerReader may have buffered beyond the header line.
	headerBytes := int64(len(headerLine))
	dataStart := headerBytes
	fileSize := info.Size()
	dataSize := fileSize - dataStart

	if dataSize <= 0 {
		return &AggregateResult{
			Stats:     make(map[string]*CampaignStats),
			TotalRows: 0,
		}, nil
	}

	// Determine number of workers
	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 1
	}

	// Calculate chunk boundaries aligned to line boundaries
	chunks, err := calculateChunks(inputPath, dataStart, fileSize, numWorkers)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate chunks: %w", err)
	}

	// Process chunks in parallel
	results := make([]workerResult, len(chunks))
	var wg sync.WaitGroup

	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, start, end int64) {
			defer wg.Done()
			results[idx] = processChunk(inputPath, start, end)
		}(i, chunk[0], chunk[1])
	}

	wg.Wait()

	// Merge all worker results
	finalStats := make(map[string]*CampaignStats)
	var totalRows, totalSkipped int64

	for _, r := range results {
		totalRows += r.rows
		totalSkipped += r.skipped

		for id, s := range r.stats {
			existing, ok := finalStats[id]
			if !ok {
				// Copy the stats to avoid sharing pointers across maps
				statsCopy := *s
				finalStats[id] = &statsCopy
			} else {
				existing.TotalImpressions += s.TotalImpressions
				existing.TotalClicks += s.TotalClicks
				existing.TotalSpend += s.TotalSpend
				existing.TotalConversions += s.TotalConversions
			}
		}
	}

	return &AggregateResult{
		Stats:       finalStats,
		TotalRows:   totalRows,
		SkippedRows: totalSkipped,
	}, nil
}

// calculateChunks divides the file into N chunks, each aligned to line boundaries.
// Returns a slice of [start, end) byte offsets.
func calculateChunks(path string, dataStart, fileSize int64, numWorkers int) ([][2]int64, error) {
	approxChunkSize := (fileSize - dataStart) / int64(numWorkers)
	if approxChunkSize < 1024 {
		// File too small to split — use a single chunk
		return [][2]int64{{dataStart, fileSize}}, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	chunks := make([][2]int64, 0, numWorkers)
	start := dataStart

	for i := 0; i < numWorkers-1; i++ {
		end := start + approxChunkSize

		if end >= fileSize {
			break
		}

		// Seek to approximate end and find the next newline
		_, err := file.Seek(end, 0)
		if err != nil {
			return nil, err
		}

		// Read a small buffer to find the newline
		buf := make([]byte, scannerBufSize)
		n, err := file.Read(buf)
		if err != nil {
			break
		}

		// Find the first newline in the buffer
		idx := bytes.IndexByte(buf[:n], '\n')
		if idx == -1 {
			// No newline found in buffer — this chunk extends to EOF
			break
		}

		// Adjust end to be right after the newline
		end = end + int64(idx) + 1

		chunks = append(chunks, [2]int64{start, end})
		start = end
	}

	// Last chunk goes to EOF
	if start < fileSize {
		chunks = append(chunks, [2]int64{start, fileSize})
	}

	return chunks, nil
}

// processChunk reads and processes a file chunk from byte offset start to end.
// Each chunk is processed independently with its own local aggregation map.
// It parses directly from bytes to minimize string allocations, and uses
// string interning for campaign IDs (only ~50 unique values).
func processChunk(path string, start, end int64) workerResult {
	result := workerResult{
		stats: make(map[string]*CampaignStats),
	}

	file, err := os.Open(path)
	if err != nil {
		return result
	}
	defer file.Close()

	// Use SectionReader to strictly limit reading to [start, end)
	section := io.NewSectionReader(file, start, end-start)
	scanner := bufio.NewScanner(section)
	scanner.Buffer(make([]byte, scannerBufSize), scannerBufSize)

	// String intern pool for campaign IDs — avoids allocating duplicate strings.
	// With ~50 unique campaigns, this saves millions of string allocations.
	internPool := make(map[string]string)

	for scanner.Scan() {
		lineBytes := scanner.Bytes()

		// Trim trailing \r for CRLF line endings
		if len(lineBytes) > 0 && lineBytes[len(lineBytes)-1] == '\r' {
			lineBytes = lineBytes[:len(lineBytes)-1]
		}
		if len(lineBytes) == 0 {
			result.skipped++
			continue
		}

		// Parse directly from bytes — no string allocation for the full line
		impressions, clicks, spend, conversions, campaignBytes, err := parseLineBytes(lineBytes)
		if err != nil {
			result.skipped++
			continue
		}

		result.rows++

		// Intern the campaign ID: look up by temporary unsafe string (no alloc),
		// only allocate a real string copy if it's a new campaign.
		tempKey := unsafe.String(unsafe.SliceData(campaignBytes), len(campaignBytes))
		campaignID, ok := internPool[tempKey]
		if !ok {
			campaignID = string(campaignBytes) // allocate only once per unique ID
			internPool[campaignID] = campaignID
		}

		s, exists := result.stats[campaignID]
		if !exists {
			s = &CampaignStats{CampaignID: campaignID}
			result.stats[campaignID] = s
		}

		s.TotalImpressions += impressions
		s.TotalClicks += clicks
		s.TotalSpend += spend
		s.TotalConversions += conversions
	}

	return result
}

// parseLineBytes parses a CSV line directly from a byte slice.
// Returns parsed numeric values and the campaign_id as a byte sub-slice
// (pointing into the scanner buffer — no allocation).
// This avoids creating any strings until we need to intern the campaign ID.
func parseLineBytes(line []byte) (impressions, clicks int64, spend float64, conversions int64, campaignID []byte, err error) {
	// Field 0: campaign_id
	idx := bytes.IndexByte(line, ',')
	if idx == -1 {
		return 0, 0, 0, 0, nil, fmt.Errorf("missing fields")
	}
	campaignID = line[:idx]
	if len(campaignID) == 0 {
		return 0, 0, 0, 0, nil, fmt.Errorf("empty campaign_id")
	}
	line = line[idx+1:]

	// Field 1: date — skip
	idx = bytes.IndexByte(line, ',')
	if idx == -1 {
		return 0, 0, 0, 0, nil, fmt.Errorf("missing fields")
	}
	line = line[idx+1:]

	// Field 2: impressions
	idx = bytes.IndexByte(line, ',')
	if idx == -1 {
		return 0, 0, 0, 0, nil, fmt.Errorf("missing fields")
	}
	impressions, err = strconv.ParseInt(unsafe.String(unsafe.SliceData(line[:idx]), idx), 10, 64)
	if err != nil {
		return 0, 0, 0, 0, nil, err
	}
	line = line[idx+1:]

	// Field 3: clicks
	idx = bytes.IndexByte(line, ',')
	if idx == -1 {
		return 0, 0, 0, 0, nil, fmt.Errorf("missing fields")
	}
	clicks, err = strconv.ParseInt(unsafe.String(unsafe.SliceData(line[:idx]), idx), 10, 64)
	if err != nil {
		return 0, 0, 0, 0, nil, err
	}
	line = line[idx+1:]

	// Field 4: spend
	idx = bytes.IndexByte(line, ',')
	if idx == -1 {
		return 0, 0, 0, 0, nil, fmt.Errorf("missing fields")
	}
	spend, err = strconv.ParseFloat(unsafe.String(unsafe.SliceData(line[:idx]), idx), 64)
	if err != nil {
		return 0, 0, 0, 0, nil, err
	}
	line = line[idx+1:]

	// Field 5: conversions (last field)
	conversions, err = strconv.ParseInt(unsafe.String(unsafe.SliceData(line), len(line)), 10, 64)
	if err != nil {
		return 0, 0, 0, 0, nil, err
	}

	return impressions, clicks, spend, conversions, campaignID, nil
}

// parseLine parses a single CSV line from a string.
// Kept for backward compatibility with tests.
func parseLine(line string) (ParsedRecord, error) {
	line = strings.TrimRight(line, "\r")
	if len(line) == 0 {
		return ParsedRecord{}, fmt.Errorf("empty line")
	}

	var fields [expectedColumns]string
	remaining := line
	for i := 0; i < expectedColumns-1; i++ {
		idx := strings.IndexByte(remaining, ',')
		if idx == -1 {
			return ParsedRecord{}, fmt.Errorf("expected %d columns", expectedColumns)
		}
		fields[i] = remaining[:idx]
		remaining = remaining[idx+1:]
	}
	fields[expectedColumns-1] = remaining

	campaignID := fields[0]
	if len(campaignID) == 0 {
		return ParsedRecord{}, fmt.Errorf("empty campaign_id")
	}

	impressions, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return ParsedRecord{}, err
	}

	clicks, err := strconv.ParseInt(fields[3], 10, 64)
	if err != nil {
		return ParsedRecord{}, err
	}

	spend, err := strconv.ParseFloat(fields[4], 64)
	if err != nil {
		return ParsedRecord{}, err
	}

	conversions, err := strconv.ParseInt(fields[5], 10, 64)
	if err != nil {
		return ParsedRecord{}, err
	}

	return ParsedRecord{
		CampaignID:  campaignID,
		Impressions: impressions,
		Clicks:      clicks,
		Spend:       spend,
		Conversions: conversions,
	}, nil
}

// validateHeader checks that the CSV header matches the expected schema.
func validateHeader(headerLine string) error {
	headerLine = strings.TrimRight(headerLine, "\r\n")
	parts := strings.Split(headerLine, ",")

	expected := []string{"campaign_id", "date", "impressions", "clicks", "spend", "conversions"}
	if len(parts) != len(expected) {
		return fmt.Errorf("invalid header: expected %d columns, got %d", len(expected), len(parts))
	}
	for i, col := range parts {
		if strings.TrimSpace(strings.ToLower(col)) != expected[i] {
			return fmt.Errorf("invalid header column %d: expected %q, got %q", i, expected[i], col)
		}
	}
	return nil
}

// parseRow converts a raw CSV string slice into a typed ParsedRecord.
// Kept for backward compatibility with tests.
func parseRow(row []string) (ParsedRecord, error) {
	if len(row) != expectedColumns {
		return ParsedRecord{}, fmt.Errorf("expected %d columns, got %d", expectedColumns, len(row))
	}

	campaignID := strings.TrimSpace(row[0])
	if campaignID == "" {
		return ParsedRecord{}, fmt.Errorf("empty campaign_id")
	}

	impressions, err := strconv.ParseInt(strings.TrimSpace(row[2]), 10, 64)
	if err != nil {
		return ParsedRecord{}, fmt.Errorf("invalid impressions %q: %w", row[2], err)
	}

	clicks, err := strconv.ParseInt(strings.TrimSpace(row[3]), 10, 64)
	if err != nil {
		return ParsedRecord{}, fmt.Errorf("invalid clicks %q: %w", row[3], err)
	}

	spend, err := strconv.ParseFloat(strings.TrimSpace(row[4]), 64)
	if err != nil {
		return ParsedRecord{}, fmt.Errorf("invalid spend %q: %w", row[4], err)
	}

	conversions, err := strconv.ParseInt(strings.TrimSpace(row[5]), 10, 64)
	if err != nil {
		return ParsedRecord{}, fmt.Errorf("invalid conversions %q: %w", row[5], err)
	}

	return ParsedRecord{
		CampaignID:  campaignID,
		Impressions: impressions,
		Clicks:      clicks,
		Spend:       spend,
		Conversions: conversions,
	}, nil
}

// ComputeResults converts aggregated stats into CampaignResults
// with CTR and CPA calculated.
func ComputeResults(stats map[string]*CampaignStats) []CampaignResult {
	results := make([]CampaignResult, 0, len(stats))

	for _, s := range stats {
		result := CampaignResult{
			CampaignID:       s.CampaignID,
			TotalImpressions: s.TotalImpressions,
			TotalClicks:      s.TotalClicks,
			TotalSpend:       s.TotalSpend,
			TotalConversions: s.TotalConversions,
		}

		// CTR = total_clicks / total_impressions
		if s.TotalImpressions > 0 {
			result.CTR = float64(s.TotalClicks) / float64(s.TotalImpressions)
		}

		// CPA = total_spend / total_conversions (nil if conversions = 0)
		if s.TotalConversions > 0 {
			cpa := s.TotalSpend / float64(s.TotalConversions)
			result.CPA = &cpa
		}

		results = append(results, result)
	}

	return results
}

// TopByCTR returns the top N campaigns sorted by CTR in descending order.
func TopByCTR(results []CampaignResult, n int) []CampaignResult {
	sorted := make([]CampaignResult, len(results))
	copy(sorted, results)

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].CTR != sorted[j].CTR {
			return sorted[i].CTR > sorted[j].CTR
		}
		return sorted[i].CampaignID < sorted[j].CampaignID
	})

	if n > len(sorted) {
		n = len(sorted)
	}
	return sorted[:n]
}

// TopByCPA returns the top N campaigns sorted by CPA in ascending order.
// Campaigns with zero conversions (CPA = nil) are excluded.
func TopByCPA(results []CampaignResult, n int) []CampaignResult {
	filtered := make([]CampaignResult, 0, len(results))
	for _, r := range results {
		if r.CPA != nil {
			filtered = append(filtered, r)
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		if *filtered[i].CPA != *filtered[j].CPA {
			return *filtered[i].CPA < *filtered[j].CPA
		}
		return filtered[i].CampaignID < filtered[j].CampaignID
	})

	if n > len(filtered) {
		n = len(filtered)
	}
	return filtered[:n]
}
