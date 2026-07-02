package aggregator

// CampaignStats holds the running totals for a single campaign during aggregation.
type CampaignStats struct {
	CampaignID       string
	TotalImpressions int64
	TotalClicks      int64
	TotalSpend       float64
	TotalConversions int64
}

// CampaignResult holds the final computed metrics for a campaign,
// including CTR and CPA derived from the aggregated stats.
type CampaignResult struct {
	CampaignID       string
	TotalImpressions int64
	TotalClicks      int64
	TotalSpend       float64
	TotalConversions int64
	CTR              float64
	CPA              *float64 // nil when conversions = 0
}

// ParsedRecord represents a single validated row from the CSV file,
// ready for aggregation.
type ParsedRecord struct {
	CampaignID  string
	Impressions int64
	Clicks      int64
	Spend       float64
	Conversions int64
}
