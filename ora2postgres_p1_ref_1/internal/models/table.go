package models

// TableMapping represents the mapping between source and destination tables
type TableMapping struct {
	SrcTable string
	DstTable string
}

// ColumnInfo represents column information
type ColumnInfo struct {
	Name     string
	DataType string
}

// TableSummary represents the summary of a table migration
type TableSummary struct {
	SourceTable      string
	SourceRowCount   int64
	DestinationTable string
	CopiedRowCount   int64
	Status           string
}

// MigrationConfig holds configuration for migration
type MigrationConfig struct {
	ChanQueue      int
	BatchSize      int
	LogReadedRows  int
	PartitionBy    string
	PartitionCount int
	FilterWhere    string
	ExpectedDBName string
}

// Constants for migration configuration
const (
	ChanQueue      = 100_000
	BatchSize      = 100_000
	LogReadedRows  = 100_000

	ChanQueueHasClob      = 50
	BatchSizeHasClob      = 50
	LogReadedHasClobRows  = 50
) 