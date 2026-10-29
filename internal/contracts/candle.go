package contracts

type Instrument struct {
	Ticker   string `json:"ticker"`
	FIGI     string `json:"figi"`
	Currency string `json:"currency"`
	LotSize  int    `json:"lotSize"`
}
type Candle struct {
	RunID          string     `json:"runId"`
	GroupID        string     `json:"groupId"`
	Instrument     Instrument `json:"instrument"`
	Timeframe      string     `json:"timeframe"`
	SequenceNumber int64      `json:"sequenceNumber"`
	CandleTime     string     `json:"candleTime"`
	Open           string     `json:"open"`
	High           string     `json:"high"`
	Low            string     `json:"low"`
	Close          string     `json:"close"`
	Volume         int64      `json:"volume"`
	IsWarmup       bool       `json:"isWarmup"`
	TradingDate    string     `json:"tradingDate,omitempty"`
	SessionPhase   string     `json:"sessionPhase,omitempty"`
}
