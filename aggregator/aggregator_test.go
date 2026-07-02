package aggregator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// helper to create a temp CSV file with given content and return its path.
func createTempCSV(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csv")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp CSV: %v", err)
	}
	return path
}

func TestRunPipeline_BasicAggregation(t *testing.T) {
	csv := `campaign_id,date,impressions,clicks,spend,conversions
CMP001,2025-01-01,12000,300,45.50,12
CMP002,2025-01-01,8000,120,28.00,4
CMP001,2025-01-02,14000,340,48.20,15
CMP002,2025-01-02,8500,150,31.00,5
`
	path := createTempCSV(t, csv)

	result, err := RunPipeline(path)
	if err != nil {
		t.Fatalf("RunPipeline error: %v", err)
	}

	if len(result.Stats) != 2 {
		t.Errorf("expected 2 campaigns, got %d", len(result.Stats))
	}

	cmp1 := result.Stats["CMP001"]
	if cmp1 == nil {
		t.Fatal("CMP001 not found in stats")
	}
	if cmp1.TotalImpressions != 26000 {
		t.Errorf("CMP001 impressions: expected 26000, got %d", cmp1.TotalImpressions)
	}
	if cmp1.TotalClicks != 640 {
		t.Errorf("CMP001 clicks: expected 640, got %d", cmp1.TotalClicks)
	}
	if cmp1.TotalConversions != 27 {
		t.Errorf("CMP001 conversions: expected 27, got %d", cmp1.TotalConversions)
	}

	cmp2 := result.Stats["CMP002"]
	if cmp2 == nil {
		t.Fatal("CMP002 not found in stats")
	}
	if cmp2.TotalImpressions != 16500 {
		t.Errorf("CMP002 impressions: expected 16500, got %d", cmp2.TotalImpressions)
	}
	if cmp2.TotalClicks != 270 {
		t.Errorf("CMP002 clicks: expected 270, got %d", cmp2.TotalClicks)
	}

	if result.TotalRows != 4 {
		t.Errorf("expected 4 total rows, got %d", result.TotalRows)
	}
	if result.SkippedRows != 0 {
		t.Errorf("expected 0 skipped rows, got %d", result.SkippedRows)
	}
}

func TestRunPipeline_MalformedRows(t *testing.T) {
	csv := `campaign_id,date,impressions,clicks,spend,conversions
CMP001,2025-01-01,12000,300,45.50,12
CMP001,2025-01-02,invalid,340,48.20,15
CMP002,2025-01-01,8000,120,28.00
CMP003,2025-01-01,5000,60,15.00,3
`
	path := createTempCSV(t, csv)

	result, err := RunPipeline(path)
	if err != nil {
		t.Fatalf("RunPipeline error: %v", err)
	}

	// CMP001 should only have 1 valid row (the other had "invalid" impressions)
	cmp1 := result.Stats["CMP001"]
	if cmp1 == nil {
		t.Fatal("CMP001 not found")
	}
	if cmp1.TotalImpressions != 12000 {
		t.Errorf("CMP001 impressions: expected 12000, got %d", cmp1.TotalImpressions)
	}

	// CMP003 should exist with valid data
	cmp3 := result.Stats["CMP003"]
	if cmp3 == nil {
		t.Fatal("CMP003 not found")
	}
	if cmp3.TotalImpressions != 5000 {
		t.Errorf("CMP003 impressions: expected 5000, got %d", cmp3.TotalImpressions)
	}

	// At least 1 row should be skipped (the "invalid" row)
	// The row with missing column may be parsed differently by csv.Reader
	if result.SkippedRows < 1 {
		t.Errorf("expected at least 1 skipped row, got %d", result.SkippedRows)
	}
}

func TestRunPipeline_EmptyFile(t *testing.T) {
	csv := `campaign_id,date,impressions,clicks,spend,conversions
`
	path := createTempCSV(t, csv)

	result, err := RunPipeline(path)
	if err != nil {
		t.Fatalf("RunPipeline error: %v", err)
	}

	if len(result.Stats) != 0 {
		t.Errorf("expected 0 campaigns, got %d", len(result.Stats))
	}
	if result.TotalRows != 0 {
		t.Errorf("expected 0 total rows, got %d", result.TotalRows)
	}
}

func TestRunPipeline_FileNotFound(t *testing.T) {
	_, err := RunPipeline("/nonexistent/file.csv")
	if err == nil {
		t.Fatal("expected error for non-existent file, got nil")
	}
}

func TestComputeResults_CTR(t *testing.T) {
	stats := map[string]*CampaignStats{
		"CMP001": {
			CampaignID:       "CMP001",
			TotalImpressions: 10000,
			TotalClicks:      500,
			TotalSpend:       100.00,
			TotalConversions: 10,
		},
	}

	results := ComputeResults(stats)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	expectedCTR := 0.05 // 500 / 10000
	if results[0].CTR != expectedCTR {
		t.Errorf("CTR: expected %f, got %f", expectedCTR, results[0].CTR)
	}
}

func TestComputeResults_CPA_ZeroConversions(t *testing.T) {
	stats := map[string]*CampaignStats{
		"CMP001": {
			CampaignID:       "CMP001",
			TotalImpressions: 10000,
			TotalClicks:      500,
			TotalSpend:       100.00,
			TotalConversions: 0,
		},
	}

	results := ComputeResults(stats)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].CPA != nil {
		t.Errorf("CPA should be nil for zero conversions, got %f", *results[0].CPA)
	}
}

func TestComputeResults_CPA_WithConversions(t *testing.T) {
	stats := map[string]*CampaignStats{
		"CMP001": {
			CampaignID:       "CMP001",
			TotalImpressions: 10000,
			TotalClicks:      500,
			TotalSpend:       200.00,
			TotalConversions: 10,
		},
	}

	results := ComputeResults(stats)
	if results[0].CPA == nil {
		t.Fatal("CPA should not be nil")
	}

	expectedCPA := 20.0 // 200.00 / 10
	if *results[0].CPA != expectedCPA {
		t.Errorf("CPA: expected %f, got %f", expectedCPA, *results[0].CPA)
	}
}

func TestComputeResults_ZeroImpressions(t *testing.T) {
	stats := map[string]*CampaignStats{
		"CMP001": {
			CampaignID:       "CMP001",
			TotalImpressions: 0,
			TotalClicks:      0,
			TotalSpend:       0,
			TotalConversions: 0,
		},
	}

	results := ComputeResults(stats)
	if results[0].CTR != 0 {
		t.Errorf("CTR should be 0 for zero impressions, got %f", results[0].CTR)
	}
}

func TestTopByCTR_Sorting(t *testing.T) {
	results := []CampaignResult{
		{CampaignID: "CMP001", CTR: 0.03},
		{CampaignID: "CMP002", CTR: 0.05},
		{CampaignID: "CMP003", CTR: 0.04},
		{CampaignID: "CMP004", CTR: 0.01},
		{CampaignID: "CMP005", CTR: 0.02},
	}

	top3 := TopByCTR(results, 3)

	if len(top3) != 3 {
		t.Fatalf("expected 3 results, got %d", len(top3))
	}
	if top3[0].CampaignID != "CMP002" {
		t.Errorf("expected CMP002 first, got %s", top3[0].CampaignID)
	}
	if top3[1].CampaignID != "CMP003" {
		t.Errorf("expected CMP003 second, got %s", top3[1].CampaignID)
	}
	if top3[2].CampaignID != "CMP001" {
		t.Errorf("expected CMP001 third, got %s", top3[2].CampaignID)
	}
}

func TestTopByCTR_LessThanN(t *testing.T) {
	results := []CampaignResult{
		{CampaignID: "CMP001", CTR: 0.05},
		{CampaignID: "CMP002", CTR: 0.03},
	}

	top10 := TopByCTR(results, 10)
	if len(top10) != 2 {
		t.Errorf("expected 2 results when only 2 exist, got %d", len(top10))
	}
}

func TestTopByCPA_Sorting(t *testing.T) {
	cpa10 := 10.0
	cpa15 := 15.0
	cpa20 := 20.0
	cpa25 := 25.0

	results := []CampaignResult{
		{CampaignID: "CMP001", CPA: &cpa20},
		{CampaignID: "CMP002", CPA: &cpa10},
		{CampaignID: "CMP003", CPA: nil},       // should be excluded
		{CampaignID: "CMP004", CPA: &cpa15},
		{CampaignID: "CMP005", CPA: &cpa25},
	}

	top3 := TopByCPA(results, 3)

	if len(top3) != 3 {
		t.Fatalf("expected 3 results, got %d", len(top3))
	}
	if top3[0].CampaignID != "CMP002" {
		t.Errorf("expected CMP002 first (CPA=10), got %s", top3[0].CampaignID)
	}
	if top3[1].CampaignID != "CMP004" {
		t.Errorf("expected CMP004 second (CPA=15), got %s", top3[1].CampaignID)
	}
	if top3[2].CampaignID != "CMP001" {
		t.Errorf("expected CMP001 third (CPA=20), got %s", top3[2].CampaignID)
	}
}

func TestTopByCPA_ExcludesZeroConversions(t *testing.T) {
	cpa10 := 10.0

	results := []CampaignResult{
		{CampaignID: "CMP001", CPA: &cpa10},
		{CampaignID: "CMP002", CPA: nil},
		{CampaignID: "CMP003", CPA: nil},
	}

	top := TopByCPA(results, 10)
	if len(top) != 1 {
		t.Errorf("expected 1 result (excluding nil CPAs), got %d", len(top))
	}
}

func TestWriteCSV_Format(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "test_output.csv")

	cpa20 := 20.0
	results := []CampaignResult{
		{
			CampaignID:       "CMP001",
			TotalImpressions: 125000,
			TotalClicks:      6250,
			TotalSpend:       12500.50,
			TotalConversions: 625,
			CTR:              0.05,
			CPA:              &cpa20,
		},
	}

	if err := WriteCSV(outputPath, results); err != nil {
		t.Fatalf("WriteCSV error: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines (header + 1 data), got %d", len(lines))
	}

	// Check header
	expectedHeader := "campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA"
	if strings.TrimSpace(lines[0]) != expectedHeader {
		t.Errorf("header mismatch:\n  expected: %s\n  got:      %s", expectedHeader, lines[0])
	}

	// Check data row formatting
	expectedRow := "CMP001,125000,6250,12500.50,625,0.0500,20.00"
	if strings.TrimSpace(lines[1]) != expectedRow {
		t.Errorf("data row mismatch:\n  expected: %s\n  got:      %s", expectedRow, lines[1])
	}
}

func TestWriteCSV_NullCPA(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "test_output.csv")

	results := []CampaignResult{
		{
			CampaignID:       "CMP001",
			TotalImpressions: 10000,
			TotalClicks:      500,
			TotalSpend:       100.00,
			TotalConversions: 0,
			CTR:              0.05,
			CPA:              nil,
		},
	}

	if err := WriteCSV(outputPath, results); err != nil {
		t.Fatalf("WriteCSV error: %v", err)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	// CPA should be empty (last field)
	expectedRow := "CMP001,10000,500,100.00,0,0.0500,"
	if strings.TrimSpace(lines[1]) != expectedRow {
		t.Errorf("data row mismatch:\n  expected: %s\n  got:      %s", expectedRow, lines[1])
	}
}

func TestParseRow_ValidRow(t *testing.T) {
	row := []string{"CMP001", "2025-01-01", "12000", "300", "45.50", "12"}
	record, err := parseRow(row)
	if err != nil {
		t.Fatalf("parseRow error: %v", err)
	}
	if record.CampaignID != "CMP001" {
		t.Errorf("expected CMP001, got %s", record.CampaignID)
	}
	if record.Impressions != 12000 {
		t.Errorf("expected 12000 impressions, got %d", record.Impressions)
	}
	if record.Clicks != 300 {
		t.Errorf("expected 300 clicks, got %d", record.Clicks)
	}
	if record.Spend != 45.50 {
		t.Errorf("expected 45.50 spend, got %f", record.Spend)
	}
	if record.Conversions != 12 {
		t.Errorf("expected 12 conversions, got %d", record.Conversions)
	}
}

func TestParseRow_InvalidColumns(t *testing.T) {
	row := []string{"CMP001", "2025-01-01", "12000"}
	_, err := parseRow(row)
	if err == nil {
		t.Fatal("expected error for row with wrong number of columns")
	}
}

func TestParseRow_InvalidImpressions(t *testing.T) {
	row := []string{"CMP001", "2025-01-01", "abc", "300", "45.50", "12"}
	_, err := parseRow(row)
	if err == nil {
		t.Fatal("expected error for invalid impressions")
	}
}

func TestParseRow_EmptyCampaignID(t *testing.T) {
	row := []string{"", "2025-01-01", "12000", "300", "45.50", "12"}
	_, err := parseRow(row)
	if err == nil {
		t.Fatal("expected error for empty campaign_id")
	}
}

func TestValidateHeader_Valid(t *testing.T) {
	header := "campaign_id,date,impressions,clicks,spend,conversions\n"
	if err := validateHeader(header); err != nil {
		t.Errorf("expected valid header, got error: %v", err)
	}
}

func TestValidateHeader_Invalid(t *testing.T) {
	header := "id,date,impressions,clicks,spend,conversions\n"
	if err := validateHeader(header); err == nil {
		t.Error("expected error for invalid header column name")
	}
}

func TestValidateHeader_WrongColumnCount(t *testing.T) {
	header := "campaign_id,date,impressions\n"
	if err := validateHeader(header); err == nil {
		t.Error("expected error for wrong column count")
	}
}

