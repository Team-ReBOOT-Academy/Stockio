package contracts

type Instrument struct {
	Ticker   string `json:"ticker"`
	Figi     string `json:"figi"`
	Currency string `json:"currency"`
	LotSize  int64  `json:"lotSize"`
}

type CandlePayload struct {
	RunID          string     `json:"runId"`
	GroupID        string     `json:"groupId"`
	Instrument     Instrument `json:"instrument"`
	Timeframe      string     `json:"timeframe"`
	SequenceNumber int64      `json:"sequenceNumber"`
	CandleTime     string     `json:"candleTime"`
	TradingDate    string     `json:"tradingDate"`
	SessionPhase   string     `json:"sessionPhase"`
	Open           string     `json:"open"`
	High           string     `json:"high"`
	Low            string     `json:"low"`
	Close          string     `json:"close"`
	Volume         int64      `json:"volume"`
	IsWarmup       bool       `json:"isWarmup"`
}
