package main

import (
	"context"
	"database/sql"
	"fmt"
	"html"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	_ "github.com/sijms/go-ora/v2"
)

type ColumnInfo struct {
	Name      string
	DataType  string
	Length    sql.NullInt64
	Precision sql.NullInt64
	Scale     sql.NullInt64
}

type PgColumnInfo struct {
	Name     string
	DataType string
	Length   sql.NullInt64
}

type IndexInfo struct {
	Name    string
	Columns []string
}

type ForeignKeyInfo struct {
	ConstraintName    string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
}

var typeMapping = map[string]interface{}{
	"BLOB":                        "bytea",
	"CHAR":                        "character",
	"CLOB":                        "text",
	"NUMBER":                      []string{"integer", "smallint", "bigint", "double precision", "numeric"},
	"RAW":                         "bytea",
	"VARCHAR2":                    "character varying",
	"DATE":                        "timestamp without time zone",
	"TIMESTAMP(6)":                "timestamp without time zone",
	"TIMESTAMP(6) WITH TIME ZONE": "timestamp with time zone",
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	oracleDSN := os.Getenv("ORACLE_DSN")
	oracleUser := os.Getenv("ORACLE_USER")
	oraclePassword := os.Getenv("ORACLE_PASSWORD")
	oraConnStr := fmt.Sprintf("oracle://%s:%s@%s", oracleUser, oraclePassword, oracleDSN)
	oraDB, err := sql.Open("oracle", oraConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to Oracle: %v", err)
	}
	defer oraDB.Close()

	pgConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		log.Fatalf("Error parsing PG config: %v", err)
	}
	pgConfig.ConnConfig.Host = os.Getenv("PG_HOST")
	pgConfig.ConnConfig.Port = uint16(parseEnvInt("PG_PORT"))
	pgConfig.ConnConfig.Database = os.Getenv("PG_DATABASE")
	pgConfig.ConnConfig.User = os.Getenv("PG_USER")
	pgConfig.ConnConfig.Password = os.Getenv("PG_PASSWORD")
	pgPool, err := pgxpool.NewWithConfig(context.Background(), pgConfig)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pgPool.Close()

	tables := getOracleTables(oraDB)

	htmlLogs := []string{}
	summary := [][]string{}

	for _, table := range tables {
		log.Printf("\n\U0001F50D Checking table: %s", table)
		htmlLogs = append(htmlLogs, fmt.Sprintf("<h2>\U0001F50D Checking table: %s</h2>", html.EscapeString(table)))

		oracleCols, _ := getColumnsOracle(oraDB, table)
		pgCols, _ := getColumnsPostgres(pgPool, table)
		pgColsConverted := make([]PgColumnInfo, len(pgCols))
		for i, col := range pgCols {
			pgColsConverted[i] = PgColumnInfo{
				Name:     col.Name,
				DataType: col.DataType,
				Length:   col.Length,
			}
		}
		columnCompare := compareColumns(oracleCols, pgColsConverted)

		htmlLogs = append(htmlLogs, renderColumnsHTML(columnCompare))

		oraIndexes := getIndexesOracle(oraDB, table)
		pgIndexes := getIndexesPostgres(pgPool, table)
		indexCompare := compareIndexes(oraIndexes, pgIndexes)
		htmlLogs = append(htmlLogs, renderIndexesHTML(indexCompare))

		oraFKs, _ := getForeignKeysOracle(oraDB, table)
		pgFKs, _ := getForeignKeysPostgres(pgPool, table)
		fkCompare := compareForeignKeys(oraFKs, pgFKs)
		htmlLogs = append(htmlLogs, renderForeignKeysHTML(fkCompare))

		oraNN, _ := getNotNullOracle(oraDB, table)
		pgNN, _ := getNotNullPostgres(pgPool, table)
		nnCompare := compareNotNull(oraNN, pgNN)
		htmlLogs = append(htmlLogs, renderNotNullHTML(nnCompare))

		oraPK := getPrimaryKeyColumnsOracle(oraDB, table)
		pgPK := getPrimaryKeyColumnsPostgres(pgPool, table)
		pkCompare := comparePrimaryKeys(oraPK, pgPK)
		htmlLogs = append(htmlLogs, renderPrimaryKeyHTML(pkCompare))

		hasError := hasErrors(columnCompare, indexCompare, fkCompare, nnCompare, pkCompare)
		if hasError {
			summary = append(summary, []string{table, "❌ ERROR"})
		} else {
			summary = append(summary, []string{table, "✅ OK"})
		}
	}

	htmlLogs = append(htmlLogs, renderSummaryHTML(summary))

	htmlReport := "<html><head><meta charset='UTF-8'><title>Comparison Report</title></head><body>" +
		strings.Join(htmlLogs, "\n") + "</body></html>"
	os.WriteFile("comparison_report.html", []byte(htmlReport), 0644)
	log.Println("\n📁 HTML report saved to comparison_report.html")
}

func parseEnvInt(key string) int {
	val := os.Getenv(key)
	i, err := strconv.Atoi(val)
	if err != nil {
		log.Fatalf("Invalid int for %s: %v", key, err)
	}
	return i
}

// Fetch tables from Oracle
func getOracleTables(db *sql.DB) []string {
	rows, err := db.Query("SELECT table_name FROM user_tables where table_name LIKE '%MS_ACCOUNT%' ORDER BY table_name")
	if err != nil {
		log.Fatalf("error fetching Oracle tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			log.Fatalf("error scanning Oracle table: %v", err)
		}
		tables = append(tables, table)
	}
	return tables
}

// Fetch columns from Oracle
// Fetch columns from Oracle
func getColumnsOracle(db *sql.DB, table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf(`
        SELECT column_name, data_type, data_length, data_precision, data_scale
        FROM user_tab_columns
        WHERE table_name = '%s'`, table)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error fetching columns for table %s from oracle: %v", table, err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		if err := rows.Scan(&col.Name, &col.DataType, &col.Length, &col.Precision, &col.Scale); err != nil {
			return nil, fmt.Errorf("error scanning oracle column: %v", err)
		}
		columns = append(columns, col)
	}
	return columns, nil
}

// Fetch columns from PostgreSQL
func getColumnsPostgres(db *pgxpool.Pool, table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf(`
        SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_name = lower('%s')`, table)

	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("error fetching columns for table %s from PostgreSQL: %v", table, err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		if err := rows.Scan(&col.Name, &col.DataType, &col.Length, &col.Precision, &col.Scale); err != nil {
			return nil, fmt.Errorf("error scanning PostgreSQL column: %v", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over PostgreSQL columns: %v", err)
	}

	return columns, nil
}

// Compare columns between Oracle and PostgreSQL
func compareColumns(oracleCols []ColumnInfo, pgCols []PgColumnInfo) string {
	var result string
	for _, oCol := range oracleCols {
		matched := false
		for _, pCol := range pgCols {
			// So sánh tên cột
			if strings.EqualFold(oCol.Name, pCol.Name) {
				matched = true

				// Ánh xạ kiểu dữ liệu Oracle sang PostgreSQL và so sánh
				oracleType := oCol.DataType

				// Chuyển sql.NullInt64 thành int nếu có giá trị hợp lệ
				var precision, scale *int
				if oCol.Precision.Valid {
					p := int(oCol.Precision.Int64)
					precision = &p
				}
				if oCol.Scale.Valid {
					s := int(oCol.Scale.Int64)
					scale = &s
				}

				// Sử dụng mapOracleToPostgresType để lấy kiểu dữ liệu PostgreSQL
				mappedPgType := mapOracleToPostgresType(oracleType, precision, scale)
				if mappedPgType != pCol.DataType {
					result += fmt.Sprintf("Column %s type mismatch: Oracle(%s) vs PostgreSQL(%s) (Mapped: '%s')\n", oCol.Name, oCol.DataType, pCol.DataType, mappedPgType)
				}

				// Bỏ qua so sánh độ dài nếu kiểu dữ liệu là ngày giờ
				lowerType := strings.ToLower(mappedPgType)
				if !strings.Contains(lowerType, "date") && !strings.Contains(lowerType, "time") {
					if oCol.Length.Int64 != pCol.Length.Int64 {
						result += fmt.Sprintf("Column %s length mismatch: Oracle(%d) vs PostgreSQL(%d)\n", oCol.Name, oCol.Length.Int64, pCol.Length.Int64)
					}
				}
			}
		}

		// Nếu cột tồn tại trong Oracle nhưng không có trong PostgreSQL
		if !matched {
			result += fmt.Sprintf("Column %s exists in Oracle but not in PostgreSQL\n", oCol.Name)
		}
	}

	// Kiểm tra cột tồn tại trong PostgreSQL nhưng không có trong Oracle
	for _, pCol := range pgCols {
		matched := false
		for _, oCol := range oracleCols {
			if strings.EqualFold(oCol.Name, pCol.Name) {
				matched = true
				break
			}
		}
		if !matched {
			result += fmt.Sprintf("Column %s exists in PostgreSQL but not in Oracle\n", pCol.Name)
		}
	}

	return result
}

// Compare indexes between Oracle and PostgreSQL
func compareIndexes(oraIndexes, pgIndexes []IndexInfo) string {
	var result string
	for _, oraIndex := range oraIndexes {
		matched := false
		for _, pgIndex := range pgIndexes {
			if strings.EqualFold(oraIndex.Name+"_IDX", pgIndex.Name) {
				matched = true
				if !equal(oraIndex.Columns, pgIndex.Columns) {
					result += fmt.Sprintf("Index %s column mismatch: Oracle(%v) vs PostgreSQL(%v)\n", oraIndex.Name, oraIndex.Columns, pgIndex.Columns)
				}
			}
		}
		if !matched {
			result += fmt.Sprintf("Index %s exists in Oracle but not in PostgreSQL\n", oraIndex.Name)
		}
	}
	for _, pgIndex := range pgIndexes {
		matched := false
		for _, oraIndex := range oraIndexes {
			if strings.EqualFold(oraIndex.Name+"_IDX", pgIndex.Name) {
				matched = true
			}
		}
		if !matched {
			result += fmt.Sprintf("Index %s exists in PostgreSQL but not in Oracle\n", pgIndex.Name)
		}
	}
	return result
}

// Compare foreign keys between Oracle and PostgreSQL
func compareForeignKeys(oraFKs, pgFKs []ForeignKeyInfo) string {
	var result string
	oraMap := make(map[string]ForeignKeyInfo)
	pgMap := make(map[string]ForeignKeyInfo)

	for _, fk := range oraFKs {
		oraMap[strings.ToUpper(fk.ConstraintName)] = fk
	}
	for _, fk := range pgFKs {
		pgMap[strings.ToUpper(fk.ConstraintName)] = fk
	}

	// So sánh các khóa ngoại trong Oracle với PostgreSQL
	for name, oraFK := range oraMap {
		pgFK, exists := pgMap[name]
		if !exists {
			result += fmt.Sprintf("Foreign key %s exists in Oracle but not in PostgreSQL\n", oraFK.ConstraintName)
			continue
		}
		if !equal(oraFK.Columns, pgFK.Columns) {
			result += fmt.Sprintf("Foreign key %s column mismatch: Oracle(%v) vs PostgreSQL(%v)\n", oraFK.ConstraintName, oraFK.Columns, pgFK.Columns)
		}
		if !strings.EqualFold(oraFK.ReferencedTable, pgFK.ReferencedTable) {
			result += fmt.Sprintf("Foreign key %s referenced table mismatch: Oracle(%s) vs PostgreSQL(%s)\n", oraFK.ConstraintName, oraFK.ReferencedTable, pgFK.ReferencedTable)
		}
	}

	// Tìm khóa ngoại có trong PostgreSQL nhưng không có trong Oracle
	for name, pgFK := range pgMap {
		if _, exists := oraMap[name]; !exists {
			result += fmt.Sprintf("Foreign key %s exists in PostgreSQL but not in Oracle\n", pgFK.ConstraintName)
		}
	}

	return result
}

// Compare Not Null constraints between Oracle and PostgreSQL
func compareNotNull(oraNN, pgNN map[string]bool) string {
	var result string
	for column, oraNotNull := range oraNN {
		if pgNotNull, exists := pgNN[strings.ToLower(column)]; exists {
			if oraNotNull != pgNotNull {
				result += fmt.Sprintf("Column %s not null mismatch: Oracle(%v) vs PostgreSQL(%v)\n", column, oraNotNull, pgNotNull)
			}
		} else {
			result += fmt.Sprintf("Column %s exists in Oracle but not in PostgreSQL\n", column)
		}
	}
	for column := range pgNN {
		if _, exists := oraNN[strings.ToUpper(column)]; !exists {
			result += fmt.Sprintf("Column %s exists in PostgreSQL but not in Oracle\n", column)
		}
	}
	return result
}

// Helper function to compare two slices of strings
func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

func getNotNullOracle(db *sql.DB, table string) (map[string]bool, error) {
	query := fmt.Sprintf(`SELECT column_name 
						  FROM all_tab_columns WHERE table_name = '%s' AND nullable = 'N'`, table)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Error fetching not null columns for table %s from Oracle: %v", table, err)
	}
	defer rows.Close()

	columns := make(map[string]bool)
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("Error scanning Oracle not null column: %v", err)
		}
		columns[colName] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Error iterating over Oracle columns: %v", err)
	}

	return columns, nil
}

func getNotNullPostgres(db *pgxpool.Pool, table string) (map[string]bool, error) {
	// PostgreSQL query to fetch not null columns
	query := fmt.Sprintf(`SELECT a.attname AS column_name
        FROM pg_attribute a
        LEFT JOIN pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
        WHERE a.attrelid = 'public.%s'::regclass
        AND a.attnum > 0
        AND NOT a.attisdropped
        AND a.attnotnull = true`, table)

	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("Error fetching not null columns for table %s from PostgreSQL: %v", table, err)
	}
	defer rows.Close()

	columns := make(map[string]bool)
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("Error scanning PostgreSQL not null column: %v", err)
		}
		columns[colName] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Error iterating over PostgreSQL columns: %v", err)
	}

	return columns, nil
}

// Fetch indexes from Oracle
func getIndexesOracle(db *sql.DB, table string) []IndexInfo {
	query := fmt.Sprintf(`
        SELECT index_name, column_name
        FROM user_ind_columns
        WHERE table_name = '%s'
        ORDER BY index_name, column_position`, table)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error fetching indexes for table %s from Oracle: %v", table, err)
	}
	defer rows.Close()

	indexMap := make(map[string][]string)
	for rows.Next() {
		var indexName, columnName string
		if err := rows.Scan(&indexName, &columnName); err != nil {
			log.Fatalf("Error scanning Oracle index: %v", err)
		}
		indexMap[indexName] = append(indexMap[indexName], strings.ToUpper(columnName))
	}

	var indexes []IndexInfo
	for name, columns := range indexMap {
		indexes = append(indexes, IndexInfo{Name: name, Columns: columns})
	}
	return indexes
}

func getIndexesPostgres(db *pgxpool.Pool, table string) []IndexInfo {
	query := fmt.Sprintf(`
        SELECT i.relname, a.attname
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE t.relname = lower('%s')
        ORDER BY i.relname, array_position(ix.indkey, a.attnum)`, table)

	rows, err := db.Query(context.Background(), query)
	if err != nil {
		log.Fatalf("Error fetching indexes for table %s from PostgreSQL: %v", table, err)
	}
	defer rows.Close()

	indexMap := make(map[string][]string)
	for rows.Next() {
		var indexName, columnName string
		if err := rows.Scan(&indexName, &columnName); err != nil {
			log.Fatalf("Error scanning PostgreSQL index: %v", err)
		}
		indexMap[strings.ToUpper(indexName)] = append(indexMap[strings.ToUpper(indexName)], strings.ToUpper(columnName))
	}

	var indexes []IndexInfo
	for name, columns := range indexMap {
		indexes = append(indexes, IndexInfo{Name: name, Columns: columns})
	}
	return indexes
}

// Fetch foreign keys from Oracle
func getForeignKeysOracle(db *sql.DB, table string) ([]ForeignKeyInfo, error) {
	query := fmt.Sprintf(`
        SELECT a.column_name, c_pk.table_name AS referenced_table, b.column_name AS referenced_column
        FROM user_constraints c
        JOIN user_cons_columns a ON c.constraint_name = a.constraint_name
        JOIN user_constraints c_pk ON c.r_constraint_name = c_pk.constraint_name
        JOIN user_cons_columns b ON c_pk.constraint_name = b.constraint_name AND a.position = b.position
        WHERE c.constraint_type = 'R' AND c.table_name = '%s'
        ORDER BY a.column_name`, table)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Error fetching foreign keys for table %s from Oracle: %v", table, err)
	}
	defer rows.Close()

	var foreignKeys []ForeignKeyInfo
	for rows.Next() {
		var columnName, referencedTable, referencedColumn string
		if err := rows.Scan(&columnName, &referencedTable, &referencedColumn); err != nil {
			return nil, fmt.Errorf("Error scanning Oracle foreign key: %v", err)
		}

		foreignKeys = append(foreignKeys, ForeignKeyInfo{
			Columns:           []string{columnName},
			ReferencedTable:   referencedTable,
			ReferencedColumns: []string{referencedColumn},
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Error iterating over Oracle foreign keys: %v", err)
	}

	return foreignKeys, nil
}

// Fetch foreign keys from PostgreSQL
func getForeignKeysPostgres(db *pgxpool.Pool, table string) ([]ForeignKeyInfo, error) {
	query := fmt.Sprintf(`
        SELECT
            att2.attname AS column_name,
            cl.relname AS referenced_table,
            att.attname AS referenced_column
        FROM
            pg_constraint con
        JOIN pg_class tbl ON tbl.oid = con.conrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS cols(colid, ord)
        JOIN pg_attribute att2 ON att2.attrelid = con.conrelid AND att2.attnum = cols.colid
        CROSS JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS refcols(colid, ord)
        JOIN pg_attribute att ON att.attrelid = con.confrelid AND att.attnum = refcols.colid AND refcols.ord = cols.ord
        JOIN pg_class cl ON cl.oid = con.confrelid
        WHERE 
            con.contype = 'f'
            AND tbl.relname = lower('%s')
            AND tbl.relkind IN ('r', 'p')
            AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = tbl.oid)
            AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = cl.oid)    
        ORDER BY att2.attname`, table)

	rows, err := db.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("Error fetching foreign keys for table %s from PostgreSQL: %v", table, err)
	}
	defer rows.Close()

	var foreignKeys []ForeignKeyInfo
	for rows.Next() {
		var columnName, referencedTable, referencedColumn string
		if err := rows.Scan(&columnName, &referencedTable, &referencedColumn); err != nil {
			return nil, fmt.Errorf("Error scanning PostgreSQL foreign key: %v", err)
		}

		foreignKeys = append(foreignKeys, ForeignKeyInfo{
			Columns:           []string{columnName},
			ReferencedTable:   referencedTable,
			ReferencedColumns: []string{referencedColumn},
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Error iterating over PostgreSQL foreign keys: %v", err)
	}

	return foreignKeys, nil
}

func getPrimaryKeyColumnsOracle(db *sql.DB, table string) []string {
	query := fmt.Sprintf(`
        SELECT cols.column_name
        FROM user_constraints cons
        JOIN user_cons_columns cols ON cons.constraint_name = cols.constraint_name
        WHERE cons.constraint_type = 'P' AND cons.table_name = '%s'
        ORDER BY cols.position`, table)

	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error fetching primary key for table %s from Oracle: %v", table, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			log.Fatalf("Error scanning Oracle primary key column: %v", err)
		}
		columns = append(columns, strings.ToUpper(col))
	}
	return columns
}

func getPrimaryKeyColumnsPostgres(db *pgxpool.Pool, table string) []string {
	query := fmt.Sprintf(`
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = lower('%s')::regclass AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)`, table)

	rows, err := db.Query(context.Background(), query)
	if err != nil {
		log.Fatalf("Error fetching primary key for table %s from PostgreSQL: %v", table, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			log.Fatalf("Error scanning PostgreSQL primary key column: %v", err)
		}
		columns = append(columns, strings.ToUpper(col))
	}
	return columns
}

func comparePrimaryKeys(pkOracle, pkPostgres []string) string {
	if len(pkOracle) != len(pkPostgres) {
		return fmt.Sprintf(`<h4 style='color:red'>Primary key length mismatch: Oracle %v vs Postgres %v</h4>`, pkOracle, pkPostgres)
	}
	for i := range pkOracle {
		if pkOracle[i] != pkPostgres[i] {
			return fmt.Sprintf(`<h4 style='color:red'>Primary key mismatch at position %d: Oracle '%s' vs Postgres '%s'</h4>`, i+1, pkOracle[i], pkPostgres[i])
		}
	}
	return `<h4 style='color:green'>Primary key match</h4>`
}

// Rendering HTML for various sections
func renderColumnsHTML(columnCompare string) string {
	return fmt.Sprintf("<h3>Column Comparison</h3><pre>%s</pre>", html.EscapeString(columnCompare))
}

func renderIndexesHTML(indexCompare string) string {
	return fmt.Sprintf("<h3>Index Comparison</h3><pre>%s</pre>", html.EscapeString(indexCompare))
}

func renderForeignKeysHTML(fkCompare string) string {
	return fmt.Sprintf("<h3>Foreign Key Comparison</h3><pre>%s</pre>", html.EscapeString(fkCompare))
}

func renderNotNullHTML(nnCompare string) string {
	return fmt.Sprintf("<h3>Not Null Comparison</h3><pre>%s</pre>", html.EscapeString(nnCompare))
}

func renderSummaryHTML(summary [][]string) string {
	var result string
	for _, row := range summary {
		result += fmt.Sprintf("<tr><td>%s</td><td>%s</td></tr>", row[0], row[1])
	}
	return fmt.Sprintf("<h3>Summary</h3><table>%s</table>", result)
}

func renderPrimaryKeyHTML(pkCompare string) string {
	return fmt.Sprintf("<h3>Not Null Comparison</h3><pre>%s</pre>", html.EscapeString(pkCompare))
}

func mapOracleToPostgresType(oracleType string, precision, scale *int) string {
	typeMapping := map[string]interface{}{
		"BLOB":                        "bytea",
		"CHAR":                        "character",
		"CLOB":                        "text",
		"NUMBER":                      []string{"integer", "smallint", "bigint", "double precision", "numeric"},
		"RAW":                         "bytea",
		"VARCHAR2":                    "character varying",
		"DATE":                        "timestamp without time zone",
		"TIMESTAMP(6)":                "timestamp without time zone",
		"TIMESTAMP(6) WITH TIME ZONE": "timestamp with time zone",
	}

	oracleType = strings.ToUpper(oracleType)
	pgType, exists := typeMapping[oracleType]
	if !exists {
		return "text"
	}

	switch v := pgType.(type) {
	case string:
		return v
	case []string:
		if precision != nil && scale != nil && *scale > 0 {
			return "numeric"
		}
		if precision != nil {
			p := *precision
			switch {
			case p <= 4:
				return "smallint"
			case p < 10:
				return "integer"
			case p < 19:
				return "bigint"
			default:
				return "numeric"
			}
		}
		return "numeric"
	default:
		return "text"
	}
}

func hasErrors(columns, indexes, foreignKeys, notNull string, pkCompare string) bool {
	return columns != "" || indexes != "" || foreignKeys != "" || notNull != "" || pkCompare != ""
}
