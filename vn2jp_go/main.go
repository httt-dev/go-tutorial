package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/abadojack/whatlanggo"
	"github.com/joho/godotenv"
	"github.com/xuri/excelize/v2"
)

type ChatRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature float32   `json:"temperature"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatResponse struct {
	Choices []struct {
		Message Message `json:"message"`
	} `json:"choices"`
}

// Gemini specific structures
type GeminiContent struct {
	Parts []GeminiPart `json:"parts"`
	Role  string       `json:"role"`
}

type GeminiPart struct {
	Text string `json:"text"`
}

type GeminiRequest struct {
	Contents         []GeminiContent `json:"contents"`
	GenerationConfig struct {
		Temperature float64 `json:"temperature"`
	} `json:"generationConfig"`
}

type GeminiResponse struct {
	Candidates []struct {
		Content GeminiContent `json:"content"`
	} `json:"candidates"`
	PromptFeedback struct {
		BlockReason string `json:"blockReason"`
	} `json:"promptFeedback"`
	Error *struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
		Status  string `json:"status"`
	} `json:"error,omitempty"`
}

type CacheEntry struct {
	VIText     string
	JPText     string
	UsageCount int
}

const (
	CacheFilePath   = "translation_cache.csv"
	MinUsageToStore = 2
)

func loadEnv() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Lỗi khi đọc file .env")
	}
}

func isVietnamese(text string) bool {
	info := whatlanggo.Detect(text)
	return info.Lang == whatlanggo.Vie && info.IsReliable()
}

func isVietnameseHeuristic(text string) bool {
	text = strings.ToLower(text)
	vietnameseCharacters := "àáảãạăắằẳẵặâấầẩẫậèéẻẽẹêếềểễệ" +
		"ìíỉĩịòóỏõọôốồổỗộơớờởỡợ" +
		"ùúủũụưứừửữựỳýỷỹỵđ"
	return strings.ContainsAny(text, vietnameseCharacters)
}

func loadCache() (map[string]*CacheEntry, error) {
	cache := make(map[string]*CacheEntry)
	file, err := os.Open(CacheFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return cache, nil
		}
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		if len(record) < 3 {
			continue
		}
		count, _ := strconv.Atoi(record[2])
		cache[record[0]] = &CacheEntry{record[0], record[1], count}
	}
	return cache, nil
}

func saveCache(cache map[string]*CacheEntry) error {
	file, err := os.Create(CacheFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, entry := range cache {
		if entry.UsageCount >= MinUsageToStore {
			writer.Write([]string{entry.VIText, entry.JPText, strconv.Itoa(entry.UsageCount)})
		}
	}
	return nil
}

func translateWithCache(openaiKey, deeplKey, libreKey, deepSeekKey, geminiKey string, text string, cache map[string]*CacheEntry) string {
	if entry, exists := cache[text]; exists {
		entry.UsageCount++
		return entry.JPText
	}
	translated := translateAuto(text, openaiKey, deeplKey, libreKey, deepSeekKey, geminiKey)
	if translated == text {
		log.Printf("Không thể dịch: %s", text)
		return text
	}

	cache[text] = &CacheEntry{text, translated, 1}
	return translated

}

func translateAuto(text, openaiKey, deeplKey, libreKey, deepSeekKey, geminiKey string) string {
	// 1. Dùng OpenAI nếu có key
	// if openaiKey != "" {
	// 	result := translateWithOpenAI(openaiKey, text)
	// 	if result != "" && result != text {
	// 		return result
	// 	}
	// }

	// Hoat dong OK
	// if geminiKey != "" {
	// 	result := translateWithGemini(geminiKey, text)
	// 	if result != "" && result != text {
	// 		return result
	// 	}
	// }

	// // 2. Dùng DeepL nếu có key
	// if deeplKey != "" {
	// 	result := translateWithDeepL(deeplKey, text)
	// 	if result != "" && result != text {
	// 		return result
	// 	}
	// }

	// 3. Fallback: dùng LibreTranslate miễn phí
	// return translateWithLibre(libreKey ,text)

	// 4. Dùng DeepSeek nếu có key
	if deepSeekKey != "" {
		result := translateWithDeepSeek(deepSeekKey, text)
		if result != "" && result != text {
			return result
		}
	}

	// 5. Nếu không có dịch vụ nào khả dụng, trả về nguyên văn
	return text
}

func translateWithOpenAI(apiKey, text string) string {
	url := "https://api.openai.com/v1/chat/completions"
	requestBody := ChatRequest{
		Model: "gpt-4o", //
		Messages: []Message{{
			Role:    "user",
			Content: fmt.Sprintf("Dịch câu sau sang tiếng Nhật:\n\"%s\"", text),
		}},
		Temperature: 0.3,
	}
	jsonData, _ := json.Marshal(requestBody)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Lỗi khi gọi API: %v\n", err)
		return text
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Lỗi khi đọc response: %v", err)
		return text
	}

	// Nếu HTTP status code không phải 2xx → log lỗi chi tiết
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Error struct {
				Message string `json:"message"`
				Code    string `json:"code"`
				Type    string `json:"type"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &apiErr); err != nil {
			log.Printf("API trả về lỗi, không thể parse JSON: %v", err)
			log.Printf("Response body: %s", string(body))
		} else {
			log.Printf("Lỗi API (%s): %s", apiErr.Error.Code, apiErr.Error.Message)
		}
		return text
	}

	var result ChatResponse
	err = json.Unmarshal(body, &result)
	if err != nil || len(result.Choices) == 0 {
		log.Printf("Không thể phân tích kết quả: %v\n", err)
		return text
	}
	return result.Choices[0].Message.Content
}

func translateWithGemini(apiKey, text string) string {
	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=%s", apiKey)

	requestBody := GeminiRequest{
		Contents: []GeminiContent{
			{
				Role: "user",
				Parts: []GeminiPart{
					{Text: fmt.Sprintf("Chỉ dịch sang tiếng Nhật:\n\"%s\"", text)},
				},
			},
		},
		GenerationConfig: struct {
			Temperature float64 `json:"temperature"`
		}{
			Temperature: 0.0,
		},
	}
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		log.Printf("Lỗi JSON Marshal cho Gemini: %v", err)
		return text
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Lỗi tạo request cho Gemini: %v", err)
		return text
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Lỗi khi gọi API Gemini: %v\n", err)
		return text
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Lỗi khi đọc response từ Gemini: %v", err)
		return text
	}

	var result GeminiResponse
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Printf("Không thể phân tích kết quả Gemini: %v\nBody: %s", err, string(body))
		return text
	}

	if result.Error != nil {
		log.Printf("Lỗi từ API Gemini (%d - %s): %s", result.Error.Code, result.Error.Status, result.Error.Message)
		return text
	}

	if result.PromptFeedback.BlockReason != "" {
		log.Printf("API Gemini chặn nội dung: %s", result.PromptFeedback.BlockReason)
		return text
	}

	if len(result.Candidates) > 0 && len(result.Candidates[0].Content.Parts) > 0 {
		return result.Candidates[0].Content.Parts[0].Text
	}

	log.Printf("Không nhận được kết quả dịch từ Gemini.")
	return text
}

func translateWithLibre(text, apiKey string) string {
	url := "https://libretranslate.com/translate"

	payload := map[string]interface{}{
		"q":            text,
		"source":       "auto", // tự động nhận dạng ngôn ngữ nguồn
		"target":       "ja",   // dịch sang tiếng Nhật
		"format":       "text",
		"alternatives": 3,      // lấy 3 câu dịch gợi ý (LibreTranslate có trả về mảng alternatives)
		"api_key":      apiKey, // API key nếu có, để trống nếu không có
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Lỗi marshal JSON: %v", err)
		return text
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Lỗi tạo request: %v", err)
		return text
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Lỗi gọi API LibreTranslate: %v", err)
		return text
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Lỗi đọc body: %v", err)
		return text
	}

	// Định nghĩa struct để parse response, có trường "translatedText" hoặc "alternatives"
	var result struct {
		TranslatedText string   `json:"translatedText"`
		Alternatives   []string `json:"alternatives"`
		Error          string   `json:"error"` // trường lỗi nếu có
	}

	if err := json.Unmarshal(body, &result); err != nil {
		log.Printf("Lỗi parse JSON response: %v\nResponse: %s", err, string(body))
		return text
	}

	if result.Error != "" {
		log.Printf("Lỗi từ LibreTranslate: %s", result.Error)
		return text
	}

	// Nếu có alternatives, ưu tiên lấy câu dịch đầu tiên
	if len(result.Alternatives) > 0 {
		return result.Alternatives[0]
	}

	// Nếu không có alternatives thì lấy translatedText
	if result.TranslatedText != "" {
		return result.TranslatedText
	}

	// fallback
	return text
}

func translateWithDeepL(apiKey, text string) string {
	deepLUrl := "https://api-free.deepl.com/v2/translate"

	data := url.Values{}
	data.Set("auth_key", apiKey)
	data.Set("text", text)
	data.Set("source_lang", "VI")
	data.Set("target_lang", "JA")

	req, _ := http.NewRequest("POST", deepLUrl, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Lỗi khi gọi DeepL API: %v", err)
		return text
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var result struct {
		Translations []struct {
			Text string `json:"text"`
		} `json:"translations"`
	}
	if err := json.Unmarshal(body, &result); err != nil || len(result.Translations) == 0 {
		log.Printf("Lỗi phân tích kết quả DeepL: %v", err)
		return text
	}
	return result.Translations[0].Text
}

func translateWithDeepSeek(apiKey, text string) string {
	type Message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	payload := map[string]interface{}{
		"model": "deepseek-chat",
		"messages": []Message{
			{
				Role:    "system",
				Content: "You are a helpful assistant.",
			},
			{
				Role:    "user",
				Content: fmt.Sprintf("Dịch chính xác câu sau sang tiếng Nhật và chỉ trả về duy nhất bản dịch, không thêm bất kỳ từ ngữ, giải thích hay ký tự nào khác:\n\"%s\"", text),
			},
		},
		"stream": false,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Lỗi JSON Marshal: %v", err)
		return text
	}

	req, err := http.NewRequest("POST", "https://api.deepseek.com/chat/completions", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Lỗi tạo request: %v", err)
		return text
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Lỗi gọi API DeepSeek: %v", err)
		return text
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Lỗi đọc response body: %v", err)
		return text
	}

	// Parse response
	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Error *struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		} `json:"error,omitempty"`
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Printf("Lỗi parse JSON response: %v\nBody: %s", err, string(body))
		return text
	}

	if result.Error != nil {
		log.Printf("Lỗi từ API DeepSeek: %s (%s)", result.Error.Message, result.Error.Code)
		return text
	}

	if len(result.Choices) == 0 {
		log.Println("Không nhận được kết quả dịch.")
		return text
	}
	translated := strings.TrimSpace(result.Choices[0].Message.Content)
	return cleanTranslation(translated, text) // Truyền vào bản dịch và text gốc
}

func processFile(filePath string, openaiKey, deeplKey, libreKey, deepSeekKey, geminiKey string, cache map[string]*CacheEntry) {
	fmt.Printf("Đang xử lý file: %s\n", filePath)
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		log.Fatalf("Không thể mở file %s: %v\n", filePath, err)
	}

	for _, sheetName := range f.GetSheetList() {
		rows, err := f.GetRows(sheetName)
		if err != nil {
			continue
		}
		for rowIdx, row := range rows {
			for colIdx, cellValue := range row {
				cellAddr, _ := excelize.CoordinatesToCellName(colIdx+1, rowIdx+1)
				if strings.TrimSpace(cellValue) != "" && (isVietnamese(cellValue) || isVietnameseHeuristic(cellValue)) {
					// Truyền geminiKey vào hàm translateWithCache
					translated := translateWithCache(openaiKey, deeplKey, libreKey, deepSeekKey, geminiKey, cellValue, cache)
					fmt.Printf("Sheet %s-Dịch [%s]: %s sang tiếng Nhật thành %s\n", sheetName, cellAddr, cellValue, translated)

					newColName, _ := excelize.CoordinatesToCellName(colIdx+1, rowIdx+1)
					f.SetCellValue(sheetName, newColName, translated)
				}
			}
		}
	}
	outputFile := strings.Replace(filePath, ".xlsx", "_JP.xlsx", 1)
	if err := f.SaveAs(outputFile); err != nil {
		log.Fatalf("Không thể lưu file %s: %v\n", outputFile, err)
	}
	fmt.Printf("✔ Đã lưu: %s\n", outputFile)
}

func cleanTranslation(translated, original string) string {
	translated = strings.TrimSpace(translated) // Xóa khoảng trắng thừa

	// Kiểm tra nếu có 「 ở đầu và 」 ở cuối, nhưng text gốc KHÔNG có
	if strings.HasPrefix(translated, "「") && strings.HasSuffix(translated, "」") &&
		!strings.Contains(original, "「") && !strings.Contains(original, "」") {
		translated = strings.TrimPrefix(translated, "「")
		translated = strings.TrimSuffix(translated, "」")
	}

	// Kiểm tra nếu có " ở đầu và " ở cuối, nhưng text gốc KHÔNG có
	if strings.HasPrefix(translated, "\"") && strings.HasSuffix(translated, "\"") &&
		!strings.Contains(original, "\"") {
		translated = strings.Trim(translated, "\"")
	}

	return translated
}

func main() {
	loadEnv()
	openaiKey := os.Getenv("OPENAI_API_KEY")
	deeplKey := os.Getenv("DEEPL_API_KEY")
	libreKey := os.Getenv("LIBRE_API_KEY")
	deepSeekKey := os.Getenv("DEEPSEEK_API_KEY")
	geminiKey := os.Getenv("GEMINI_API_KEY")

	files := os.Getenv("FILES_TO_TRANSLATE")

	if openaiKey == "" || files == "" {
		log.Fatal("Vui lòng cấu hình OPENAI_API_KEY và FILES_TO_TRANSLATE trong file .env")
	}

	cache, err := loadCache()
	if err != nil {
		log.Fatalf("Lỗi khi load cache: %v", err)
	}
	defer func() {
		err := saveCache(cache)
		if err != nil {
			log.Printf("Lỗi khi lưu cache: %v", err)
		} else {
			fmt.Println("✔ Đã lưu cache vào", CacheFilePath)
		}
	}()

	fileList := strings.Split(files, ",")
	for _, file := range fileList {
		processFile(strings.TrimSpace(file), openaiKey, deeplKey, libreKey, deepSeekKey, geminiKey, cache)
	}
}
