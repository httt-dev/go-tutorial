package output

import (
	"fmt"
	"html/template"
	"os"
	"time"

	"ora2pg_data_diff_table/comparison"
	"ora2pg_data_diff_table/config"
	"ora2pg_data_diff_table/database"
)

// HTMLReport represents the data structure for the HTML report
type HTMLReport struct {
	Title            string
	Timestamp        string
	OracleTable      string
	PostgresTable    string
	TotalRecords     int
	DifferentRecords int
	Differences      []database.Record
	DiffFields       []string
	StartTime        time.Time
	EndTime          time.Time
}

// GenerateReport creates an HTML report from the comparison results
func GenerateReport(result *comparison.ComparisonResult, config *config.Config) error {
	// Create report data
	report := HTMLReport{
		Title:            "Database Comparison Report",
		Timestamp:        time.Now().Format("2006-01-02 15:04:05"),
		OracleTable:      config.TableOracle,
		PostgresTable:    config.TablePostgres,
		TotalRecords:     result.TotalRecords,
		DifferentRecords: result.DifferentRecords,
		Differences:      result.Differences,
		DiffFields:       config.CompareColumns,
		StartTime:        time.Now(),
		EndTime:          time.Now(),
	}

	// Create template with functions
	tmpl, err := template.New("report").Funcs(template.FuncMap{
		"contains": func(slice []string, item string) bool {
			for _, s := range slice {
				if s == item {
					return true
				}
			}
			return false
		},
	}).Parse(reportTemplate)
	if err != nil {
		return fmt.Errorf("error parsing template: %v", err)
	}

	// Create output file
	file, err := os.Create("comparison_report.html")
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer file.Close()

	// Execute template
	if err := tmpl.Execute(file, report); err != nil {
		return fmt.Errorf("error executing template: %v", err)
	}

	return nil
}

// Duration returns the duration of the comparison
func (r *HTMLReport) Duration() time.Duration {
	return r.EndTime.Sub(r.StartTime)
}

// FormatDuration formats the duration in a human-readable format
func (r *HTMLReport) FormatDuration() string {
	d := r.Duration()
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

// reportTemplate is the HTML template for the report
const reportTemplate = `
<!DOCTYPE html>
<html>
<head>
    <title>{{.Title}}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: #f5f5f5; padding: 20px; border-radius: 5px; }
        .summary { margin: 20px 0; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f5f5f5; }
        .diff { background-color: #fff3cd; }
        .key { background-color: #e9ecef; }
    </style>
</head>
<body>
    <div class="header">
        <h1>{{.Title}}</h1>
        <p>Generated at: {{.Timestamp}}</p>
        <p>Oracle Table: {{.OracleTable}}</p>
        <p>PostgreSQL Table: {{.PostgresTable}}</p>
    </div>

    <div class="summary">
        <h2>Summary</h2>
        <p>Total Records: {{.TotalRecords}}</p>
        <p>Different Records: {{.DifferentRecords}}</p>
        <p>Execution Time: {{.EndTime.Sub .StartTime}}</p>
    </div>

    {{if .Differences}}
    <h2>Differences</h2>
    <table>
        <tr>
            <th>Key Columns</th>
            <th>Column</th>
            <th>Oracle Value</th>
            <th>PostgreSQL Value</th>
        </tr>
        {{range .Differences}}
            {{range $key, $value := .KeyValues}}
            <tr class="key">
                <td>{{$key}}</td>
                <td>Key</td>
                <td>{{$value}}</td>
                <td>{{$value}}</td>
            </tr>
            {{end}}
            {{range $key, $value := .ColValues}}
                {{if contains $.DiffFields $key}}
                <tr class="diff">
                    <td>{{$key}}</td>
                    <td>Value</td>
                    <td>{{$value}}</td>
                    <td>{{$value}}</td>
                </tr>
                {{end}}
            {{end}}
        {{end}}
    </table>
    {{else}}
    <h2>No differences found</h2>
    {{end}}
</body>
</html>
`
