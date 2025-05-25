package main

import (
	"context"
	"database/sql/driver"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"image/color"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	go_ora "github.com/sijms/go-ora/v2"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

type DBConfig struct {
	ConnectionName string `json:"ConnectionName"`
	DbType         string `json:"DbType"`
	User           string `json:"User"`
	Password       string `json:"Password"`
	ConnectString  string `json:"ConnectString"`
	Host           string `json:"Host"`
	Port           int    `json:"Port"`
	Database       string `json:"Database"`
	Pooling        bool   `json:"Pooling"`
	MinPoolSize    int    `json:"MinPoolSize"`
	MaxPoolSize    int    `json:"MaxPoolSize"`
}

type Config struct {
	DatabaseSettings []DBConfig `json:"DatabaseSettings"`
}

type myTheme struct{}

func (m myTheme) Color(n fyne.ThemeColorName, v fyne.ThemeVariant) color.Color {
	return theme.DefaultTheme().Color(n, v)
}

func (m myTheme) Font(style fyne.TextStyle) fyne.Resource {
	return theme.DefaultTheme().Font(style)
}

func (m myTheme) Icon(name fyne.ThemeIconName) fyne.Resource {
	return theme.DefaultTheme().Icon(name)
}

func (m myTheme) Size(name fyne.ThemeSizeName) float32 {
	if name == theme.SizeNameText {
		return 16 // tăng font size mặc định
	}
	return theme.DefaultTheme().Size(name)
}

func initLogFile() *os.File {
	// Tạo thư mục nếu chưa có
	err := os.MkdirAll("log", os.ModePerm)
	if err != nil {
		log.Fatalf("Could not create log directory: %v", err)
	}

	// Tạo tên file theo định dạng yyyyMMdd_HHmmssffff.log
	now := time.Now()
	filename := now.Format("20060102_150405.0000") + ".log"
	fullPath := filepath.Join("log", filename)

	// Tạo file log
	logFile, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	// Cấu hình log chuẩn ra file
	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.Printf("Log started: %s\n", filename)
	return logFile
}

func loadConfig(file string) (*Config, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var config Config
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&config)
	return &config, err
}

func logToOutput(logWidget *widget.Entry, msg string) {
	// current := logWidget.Text
	// timestamp := time.Now().Format("15:04:05")
	// logWidget.SetText(current + timestamp + " - " + msg + "\n")
	// logWidget.Refresh()

}

func executeQueryOracle(cfg DBConfig, query, filename string, wg *sync.WaitGroup, errChan chan error, logWidget *widget.Entry) {
	defer wg.Done()

	logToOutput(logWidget, "Connecting to Oracle...")
	connStr := fmt.Sprintf("oracle://%s:%s@%s", cfg.User, cfg.Password, cfg.ConnectString)
	db, err := go_ora.NewConnection(connStr, nil)
	if err != nil {
		errChan <- err
		return
	}
	if err = db.Open(); err != nil {
		errChan <- err
		return
	}
	defer db.Close()
	logToOutput(logWidget, "Connected to Oracle")

	stmt := go_ora.NewStmt(query, db)
	defer stmt.Close()

	rs, err := stmt.Query(nil)
	if err != nil {
		errChan <- err
		return
	}
	defer rs.Close()

	log.Println("Starting export oracle data to CSV")
	err = writeCSVFromOracleRows(rs, filename)

	// done := make(chan bool)
	// go func() {

	// 	if err != nil {
	// 	log.Println("Starting export oracle data to CSV")
	// err = writeCSVFromOracleRows(rs, filename)
	// 		errChan <- err
	// 	}
	// 	done <- true
	// }()
	// <-done

	if err != nil {
		errChan <- err
	} else {
		logToOutput(logWidget, "Oracle query complete. File saved: "+filename)
	}
}

func executeQueryPostgres(cfg DBConfig, query, filename string, wg *sync.WaitGroup, errChan chan error, logWidget *widget.Entry) {
	defer wg.Done()

	logToOutput(logWidget, "Connecting to Postgres...")
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		errChan <- err
		return
	}
	defer pool.Close()
	logToOutput(logWidget, "Connected to Postgres")

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		errChan <- err
		return
	}
	defer rows.Close()

	log.Println("Starting export postgres data to CSV")
	err = writeCSVFromPgxRows(rows, filename)

	// done := make(chan bool)
	// go func() {
	// 	log.Println("Starting export postgres data to CSV")
	// 	err = writeCSVFromPgxRows(rows, filename)
	// 	if err != nil {
	// 		errChan <- err
	// 	}
	// 	done <- true
	// }()
	// <-done

	if err != nil {
		errChan <- err
	} else {
		logToOutput(logWidget, "Postgres query complete. File saved: "+filename)
	}
}

func writeCSVFromOracleRows(rs driver.Rows, filename string) error {
	colNames := rs.Columns()
	for i := range colNames {
		colNames[i] = strings.ToLower(colNames[i])
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	if err := writer.Write(colNames); err != nil {
		return err
	}

	batch := [][]string{}
	batchSize := 100000

	for {
		rowVals := make([]driver.Value, len(colNames))
		err := rs.Next(rowVals)
		if err != nil {
			break
		}

		record := make([]string, len(rowVals))
		for i, val := range rowVals {
			record[i] = fmt.Sprint(val)
		}
		batch = append(batch, record)

		if len(batch) >= batchSize {
			if err := writer.WriteAll(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := writer.WriteAll(batch); err != nil {
			return err
		}
	}
	return nil
}

func writeCSVFromPgxRows(rows pgx.Rows, filename string) error {
	colNames := rows.FieldDescriptions()
	columns := make([]string, len(colNames))
	for i, col := range colNames {
		columns[i] = strings.ToLower(string(col.Name))
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	if err := writer.Write(columns); err != nil {
		return err
	}

	batch := [][]string{}
	batchSize := 100000

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}

		record := make([]string, len(values))
		for i, val := range values {
			record[i] = fmt.Sprint(val)
		}
		batch = append(batch, record)

		if len(batch) >= batchSize {
			if err := writer.WriteAll(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := writer.WriteAll(batch); err != nil {
			return err
		}
	}
	return rows.Err()
}

func main() {
	logFile := initLogFile()
	defer logFile.Close()

	log.Println("Application started")

	a := app.New()
	a.Settings().SetTheme(myTheme{})
	w := a.NewWindow("DB Query Comparator")
	w.Resize(fyne.NewSize(900, 700))

	config, err := loadConfig("connections.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	oracleSelect := widget.NewSelect(nil, nil)
	postgresSelect := widget.NewSelect(nil, nil)
	oracleQuery := widget.NewMultiLineEntry()
	oracleQuery.TextStyle = fyne.TextStyle{Monospace: true}
	oracleQuery.Wrapping = fyne.TextWrapWord
	oracleQuery.Refresh()

	postgresQuery := widget.NewMultiLineEntry()
	postgresQuery.TextStyle = fyne.TextStyle{Monospace: true}
	postgresQuery.Wrapping = fyne.TextWrapWord
	postgresQuery.Refresh()

	logOutput := widget.NewMultiLineEntry()
	logOutput.Wrapping = fyne.TextWrapWord
	// logOutput.TextStyle = fyne.TextStyle{Monospace: true}
	logOutput.Disable()

	for _, cfg := range config.DatabaseSettings {
		if cfg.DbType == "oracle" {
			oracleSelect.Options = append(oracleSelect.Options, cfg.ConnectionName)
		} else if cfg.DbType == "postgres" {
			postgresSelect.Options = append(postgresSelect.Options, cfg.ConnectionName)
		}
	}

	btnCompare := widget.NewButton("Compare", func() {
		oracleConn := oracleSelect.Selected
		postgresConn := postgresSelect.Selected
		queryOracle := oracleQuery.Text
		queryPostgres := postgresQuery.Text
		logOutput.Text = ""

		var cfgOracle, cfgPostgres *DBConfig
		for _, c := range config.DatabaseSettings {
			if c.ConnectionName == oracleConn {
				cfgOracle = &c
			} else if c.ConnectionName == postgresConn {
				cfgPostgres = &c
			}
		}
		if cfgOracle == nil || cfgPostgres == nil {
			dialog.ShowInformation("Info", "missing config", w)
			return
		}

		if queryOracle == "" || queryPostgres == "" {
			dialog.ShowInformation("Info", "please provide both queries", w)
			return
		}

		timestamp := time.Now().Format("20060102_150405")
		os.MkdirAll("results", os.ModePerm)
		oracleFile := filepath.Join("results", fmt.Sprintf("oracle_%s.csv", timestamp))
		postgresFile := filepath.Join("results", fmt.Sprintf("postgres_%s.csv", timestamp))

		loadingDialog := dialog.NewCustomWithoutButtons("Running", widget.NewLabel("Executing queries, please wait..."), w)
		loadingDialog.Show()

		logToOutput(logOutput, "Starting query execution...")

		errChan := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)

		go executeQueryOracle(*cfgOracle, queryOracle, oracleFile, &wg, errChan, logOutput)
		go executeQueryPostgres(*cfgPostgres, queryPostgres, postgresFile, &wg, errChan, logOutput)

		go func() {
			wg.Wait()
			close(errChan)

			loadingDialog.Hide()
			hasError := false

			for err := range errChan {
				if err != nil {
					hasError = true
					logToOutput(logOutput, fmt.Sprintf("Error: %v", err))
					dialog.ShowError(err, w)
				}
			}

			if !hasError {
				logToOutput(logOutput, "Query execution completed successfully.")
				exec.Command("C:\\Program Files\\WinMerge\\WinMergeU.exe", oracleFile, postgresFile).Start()
			}
		}()
	})

	// Oracle panel: label+select cố định, query entry stretch hết chiều cao còn lại
	oracleTop := container.NewVBox(
		widget.NewLabel("Select Oracle Connection:"),
		oracleSelect,
		widget.NewLabel("Oracle Query:"),
	)
	oraclePanel := container.NewBorder(
		oracleTop, nil, nil, nil,
		container.NewMax(oracleQuery),
	)

	// Postgres panel tương tự
	postgresTop := container.NewVBox(
		widget.NewLabel("Select Postgres Connection:"),
		postgresSelect,
		widget.NewLabel("Postgres Query:"),
	)
	postgresPanel := container.NewBorder(
		postgresTop, nil, nil, nil,
		container.NewMax(postgresQuery),
	)

	// Side by side panel (Oracle | Postgres)
	topSplit := container.NewHSplit(oraclePanel, postgresPanel)
	topSplit.Offset = 0.5

	// Bottom panel: button + log, log stretch chiều cao còn lại
	// bottom := container.NewBorder(
	// 	btnCompare, // Compare button giữa, có nền màu và rộng
	// 	nil, nil, nil,
	// 	container.NewMax(logOutput),
	// )
	bottom := container.NewBorder(
		btnCompare, // Compare button giữa, có nền màu và rộng
		nil, nil, nil,
	)
	// Chính: chia dọc top/bottom
	mainSplit := container.NewVSplit(topSplit, bottom)
	mainSplit.Offset = 1

	w.SetContent(mainSplit)
	w.ShowAndRun()
}
