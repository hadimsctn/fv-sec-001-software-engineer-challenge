package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"ad-performance-aggregator/aggregator"
)

func main() {
	// Parse CLI flags
	inputPath := flag.String("input", "", "Path to the input CSV file (required)")
	outputDir := flag.String("output", "results/", "Directory to write output CSV files")
	flag.Parse()

	if *inputPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --input flag is required\n\n")
		fmt.Fprintf(os.Stderr, "Usage: aggregator --input <csv_file> --output <output_dir>\n")
		fmt.Fprintf(os.Stderr, "Example: aggregator --input ad_data.csv --output results/\n")
		os.Exit(1)
	}

	// Validate input file exists
	if _, err := os.Stat(*inputPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: input file %q does not exist\n", *inputPath)
		os.Exit(1)
	}

	// Ensure output directory exists
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create output directory %q: %v\n", *outputDir, err)
		os.Exit(1)
	}

	fmt.Printf("=== Ad Performance Aggregator ===\n")
	fmt.Printf("Input:   %s\n", *inputPath)
	fmt.Printf("Output:  %s\n", *outputDir)
	fmt.Printf("Workers: %d (CPU cores)\n", runtime.NumCPU())
	fmt.Println()

	// Force GC before measurement to get a clean baseline
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Start processing
	startTime := time.Now()

	fmt.Println("🔄 Running aggregation pipeline...")
	result, err := aggregator.RunPipeline(*inputPath)
	if err != nil {
		log.Fatalf("Aggregation failed: %v", err)
	}

	// Compute metrics (CTR, CPA)
	results := aggregator.ComputeResults(result.Stats)

	// Get Top 10 lists
	top10CTR := aggregator.TopByCTR(results, 10)
	top10CPA := aggregator.TopByCPA(results, 10)

	// Write output files
	ctrPath := filepath.Join(*outputDir, "top10_ctr.csv")
	cpaPath := filepath.Join(*outputDir, "top10_cpa.csv")

	if err := aggregator.WriteCSV(ctrPath, top10CTR); err != nil {
		log.Fatalf("Failed to write %s: %v", ctrPath, err)
	}

	if err := aggregator.WriteCSV(cpaPath, top10CPA); err != nil {
		log.Fatalf("Failed to write %s: %v", cpaPath, err)
	}

	duration := time.Since(startTime)

	// Measure peak memory
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Print results summary
	fmt.Println()
	fmt.Println("✅ Aggregation complete!")
	fmt.Println()
	fmt.Printf("📊 Statistics:\n")
	fmt.Printf("   Campaigns found:  %d\n", len(result.Stats))
	fmt.Printf("   Rows processed:   %d\n", result.TotalRows)
	fmt.Printf("   Rows skipped:     %d\n", result.SkippedRows)
	fmt.Println()
	fmt.Printf("📁 Output files:\n")
	fmt.Printf("   %s\n", ctrPath)
	fmt.Printf("   %s\n", cpaPath)
	fmt.Println()
	fmt.Printf("⚡ Performance:\n")
	fmt.Printf("   Processing time:  %s\n", duration.Round(time.Millisecond))
	fmt.Printf("   Peak memory:      %.2f MB\n", float64(memAfter.Sys-memBefore.Sys)/(1024*1024))
	fmt.Printf("   Total allocated:  %.2f MB\n", float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/(1024*1024))
	fmt.Println()

	// Print Top 10 CTR preview
	fmt.Println("📈 Top 10 CTR (preview):")
	fmt.Printf("   %-12s %18s %14s %14s %18s %10s %10s\n",
		"Campaign", "Impressions", "Clicks", "Spend", "Conversions", "CTR", "CPA")
	fmt.Println("   " + repeat("─", 100))
	for _, r := range top10CTR {
		cpaStr := ""
		if r.CPA != nil {
			cpaStr = fmt.Sprintf("%.2f", *r.CPA)
		}
		fmt.Printf("   %-12s %18d %14d %14.2f %18d %10.4f %10s\n",
			r.CampaignID, r.TotalImpressions, r.TotalClicks, r.TotalSpend,
			r.TotalConversions, r.CTR, cpaStr)
	}
	fmt.Println()

	// Print Top 10 CPA preview
	fmt.Println("💰 Top 10 CPA (preview):")
	fmt.Printf("   %-12s %18s %14s %14s %18s %10s %10s\n",
		"Campaign", "Impressions", "Clicks", "Spend", "Conversions", "CTR", "CPA")
	fmt.Println("   " + repeat("─", 100))
	for _, r := range top10CPA {
		cpaStr := ""
		if r.CPA != nil {
			cpaStr = fmt.Sprintf("%.2f", *r.CPA)
		}
		fmt.Printf("   %-12s %18d %14d %14.2f %18d %10.4f %10s\n",
			r.CampaignID, r.TotalImpressions, r.TotalClicks, r.TotalSpend,
			r.TotalConversions, r.CTR, cpaStr)
	}
}

// repeat returns a string consisting of s repeated n times.
func repeat(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}
