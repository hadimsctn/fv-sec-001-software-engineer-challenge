package aggregator

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

// WriteCSV writes a slice of CampaignResults to a CSV file with the expected
// output format:
//
//	campaign_id,total_impressions,total_clicks,total_spend,total_conversions,CTR,CPA
//
// CTR is formatted with 4 decimal places, CPA with 2 decimal places.
// If CPA is nil (conversions = 0), the CPA column is left empty.
func WriteCSV(outputPath string, results []CampaignResult) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", outputPath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"campaign_id",
		"total_impressions",
		"total_clicks",
		"total_spend",
		"total_conversions",
		"CTR",
		"CPA",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write data rows
	for _, r := range results {
		cpaStr := ""
		if r.CPA != nil {
			cpaStr = strconv.FormatFloat(*r.CPA, 'f', 2, 64)
		}

		row := []string{
			r.CampaignID,
			strconv.FormatInt(r.TotalImpressions, 10),
			strconv.FormatInt(r.TotalClicks, 10),
			strconv.FormatFloat(r.TotalSpend, 'f', 2, 64),
			strconv.FormatInt(r.TotalConversions, 10),
			strconv.FormatFloat(r.CTR, 'f', 4, 64),
			cpaStr,
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row for %s: %w", r.CampaignID, err)
		}
	}

	return nil
}
