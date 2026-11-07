package ports

type Health interface {
	SetKafka(up bool, details string)
}
