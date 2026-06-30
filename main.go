package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// TxDetail represents a parsed and calculated transaction stored in the DB
type TxDetail struct {
	Signature       string    `json:"signature"`
	BlockTime       int64     `json:"blockTime"`
	DateTime        string    `json:"dateTime"`
	Slot            uint64    `json:"slot"`
	InstructionType string    `json:"instructionType"` // "LeverageBuy", "EarlyCloseLeverage", "Initialize", etc.
	UserAddress     string    `json:"userAddress"`
	USDCAmount      float64   `json:"usdcAmount"`      // Net user deposit or net user payout
	NotionalAmount  float64   `json:"notionalAmount"`  // Leverage notional
	Leverage        int       `json:"leverage"`
	UPOnlyAmount    float64   `json:"uponlyAmount"`    // UPONLY tokens minted or burned
	Fee             float64   `json:"fee"`             // Distributed fees
	CalculatedPrice float64   `json:"calculatedPrice"` // Price AFTER this transaction
	PoolAfter       float64   `json:"poolAfter"`       // Pool size AFTER this transaction
	SupplyAfter     float64   `json:"supplyAfter"`     // Supply AFTER this transaction
	DurationDays    float64   `json:"durationDays"`    // Investment duration in days (sells only)
	ProfitAbsolute  float64   `json:"profitAbsolute"`  // Actual absolute profit in USDC (sells only)
	ProfitPercent   float64   `json:"profitPercent"`   // Profit percentage (sells only)
	BuySignature    string    `json:"buySignature"`    // Signature of matching buy transaction
}

// Stats represents overall metrics for the token
type Stats struct {
	PoolSize             float64 `json:"poolSize"`
	CirculatingSupply    float64 `json:"circulatingSupply"`
	Price                float64 `json:"price"`
	TotalTransactions    int     `json:"totalTransactions"`
	BuyCount             int     `json:"buyCount"`
	SellCount            int     `json:"sellCount"`
	TotalVolumeUSDC      float64 `json:"totalVolumeUsdc"`
	PriceGrowthPercent   float64 `json:"priceGrowthPercent"`
	Velocity24h          float64 `json:"velocity24h"`          // Price growth per day (average over last 50 transactions)
	AccelerationFactor   float64 `json:"accelerationFactor"`   // Ratio of recent velocity vs historical velocity
}

// Global server state
var (
	db             *sql.DB
	clientsMu      sync.Mutex
	clients        = make(map[chan TxDetail]bool)
	syncing        bool
	syncMu         sync.Mutex
	programAddress = "Hm6QaYdnze5BQGgzbLYeLr5yjYJnKZ7tKvbwi44ebogd"
	usdcVault      = "5sbtVipCwtp1bnGUN3jyPvWyNQQMePnJRVzQbZkAak2x"
	uponlyMint     = "BmW33QveVoohZJf7AdnGR5oWcyVjUCweh4CmchDnEzC"
)

func main() {
	// Initialize Database
	var err error
	db, err = sql.Open("sqlite", "uponly.db")
	if err != nil {
		log.Fatalf("Failed to open SQLite database: %v", err)
	}
	defer db.Close()

	if err := initSchema(); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}

	// Try to import historical raw transactions on startup if DB is empty
	go func() {
		time.Sleep(1 * time.Second)
		if count, _ := getTxCount(); count == 0 {
			log.Println("Database is empty. Importing historical transactions...")
			importRawTransactions()
			replayAllTransactions()
		} else {
			log.Printf("Database already contains %d transactions.", count)
			// Trigger a replay to ensure math is consistent
			replayAllTransactions()
		}
		// Start daily sync loop
		go startSyncLoop()
	}()

	// Serve API & Frontend
	http.HandleFunc("/", handleHome)
	http.HandleFunc("/api/history", handleHistory)
	http.HandleFunc("/api/stats", handleStats)
	http.HandleFunc("/api/sync", handleSync)
	http.HandleFunc("/api/live", handleLiveStream)

	socketPath := os.Getenv("UNIX")
	if socketPath != "" {
		_ = os.Remove(socketPath)
		listener, err := net.Listen("unix", socketPath)
		if err != nil {
			log.Fatalf("Unix socket listener failed: %v", err)
		}
		defer os.Remove(socketPath)
		if err := os.Chmod(socketPath, 0666); err != nil {
			log.Printf("Could not change permissions to 0666 on unix:%s", socketPath)
		}
		log.Printf("Starting UpOnly server on unix:%s", socketPath)
		if err := http.Serve(listener, nil); err != nil {
			log.Fatalf("Server stopped: %v", err)
		}
	} else {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		addr := fmt.Sprintf(":%s", port)
		log.Printf("Starting UpOnly server on http://localhost%s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}
}

// initSchema creates the transactions table
func initSchema() error {
	query := `
	CREATE TABLE IF NOT EXISTS transactions (
		signature TEXT PRIMARY KEY,
		block_time INTEGER NOT NULL,
		slot INTEGER NOT NULL,
		instruction_type TEXT NOT NULL,
		user_address TEXT NOT NULL,
		usdc_amount REAL NOT NULL,
		notional_amount REAL NOT NULL,
		leverage INTEGER NOT NULL,
		uponly_amount REAL NOT NULL,
		fee REAL NOT NULL,
		calculated_price REAL NOT NULL,
		pool_after REAL NOT NULL,
		supply_after REAL NOT NULL,
		duration_days REAL DEFAULT 0.0,
		profit_absolute REAL DEFAULT 0.0,
		profit_percent REAL DEFAULT 0.0,
		buy_signature TEXT DEFAULT ''
	);
	CREATE INDEX IF NOT EXISTS idx_block_time ON transactions(block_time);
	CREATE INDEX IF NOT EXISTS idx_slot ON transactions(slot);
	`
	_, err := db.Exec(query)
	if err != nil {
		return err
	}

	// Run migrations to add columns if they don't exist on older databases
	db.Exec("ALTER TABLE transactions ADD COLUMN duration_days REAL DEFAULT 0.0")
	db.Exec("ALTER TABLE transactions ADD COLUMN profit_absolute REAL DEFAULT 0.0")
	db.Exec("ALTER TABLE transactions ADD COLUMN profit_percent REAL DEFAULT 0.0")
	db.Exec("ALTER TABLE transactions ADD COLUMN buy_signature TEXT DEFAULT ''")

	return nil
}

func getTxCount() (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM transactions").Scan(&count)
	return count, err
}

// startSyncLoop triggers a background sync every 12 hours
func startSyncLoop() {
	ticker := time.NewTicker(12 * time.Hour)
	for range ticker.C {
		log.Println("Starting scheduled background sync...")
		syncLatestTransactions()
	}
}

// handleHome serves the explorer.html file
func handleHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, "explorer.html")
}

// handleHistory handles paginated transaction history
func handleHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")
	order := r.URL.Query().Get("order")
	filterType := r.URL.Query().Get("type")

	limit := 100
	offset := 0

	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
		limit = l
	}
	if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
		offset = o
	}
	if order != "asc" {
		order = "desc" // default to reverse chronological
	}

	query := "SELECT signature, block_time, slot, instruction_type, user_address, usdc_amount, notional_amount, leverage, uponly_amount, fee, calculated_price, pool_after, supply_after, duration_days, profit_absolute, profit_percent, buy_signature FROM transactions"
	var args []interface{}

	if filterType != "" {
		query += " WHERE instruction_type = ?"
		args = append(args, filterType)
	}

	query += fmt.Sprintf(" ORDER BY block_time %s, slot %s LIMIT ? OFFSET ?", order, order)
	args = append(args, limit, offset)

	rows, err := db.Query(query, args...)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var txs []TxDetail
	for rows.Next() {
		var tx TxDetail
		err := rows.Scan(
			&tx.Signature, &tx.BlockTime, &tx.Slot, &tx.InstructionType,
			&tx.UserAddress, &tx.USDCAmount, &tx.NotionalAmount, &tx.Leverage,
			&tx.UPOnlyAmount, &tx.Fee, &tx.CalculatedPrice, &tx.PoolAfter, &tx.SupplyAfter,
			&tx.DurationDays, &tx.ProfitAbsolute, &tx.ProfitPercent, &tx.BuySignature,
		)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
			return
		}
		tx.DateTime = time.Unix(tx.BlockTime, 0).Format("2006-01-02 15:04:05")
		txs = append(txs, tx)
	}

	json.NewEncoder(w).Encode(txs)
}

// handleStats handles dashboard stats request
func handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	var stats Stats

	// Get latest transaction state
	row := db.QueryRow("SELECT pool_after, supply_after, calculated_price FROM transactions ORDER BY block_time DESC, slot DESC LIMIT 1")
	err := row.Scan(&stats.PoolSize, &stats.CirculatingSupply, &stats.Price)
	if err != nil {
		// Default starting stats if no transactions exist yet
		stats.PoolSize = 1000000.0
		stats.CirculatingSupply = 1000000.0
		stats.Price = 1.0
	}

	// Total transaction count
	db.QueryRow("SELECT COUNT(*) FROM transactions").Scan(&stats.TotalTransactions)

	// Buy and Sell count
	db.QueryRow("SELECT COUNT(*) FROM transactions WHERE instruction_type = 'LeverageBuy'").Scan(&stats.BuyCount)
	db.QueryRow("SELECT COUNT(*) FROM transactions WHERE instruction_type = 'EarlyCloseLeverage'").Scan(&stats.SellCount)

	// Total USDC Volume
	db.QueryRow("SELECT COALESCE(SUM(usdc_amount), 0) FROM transactions").Scan(&stats.TotalVolumeUSDC)

	stats.PriceGrowthPercent = (stats.Price - 1.0) * 100.0

	// Calculate velocity of price growth
	// Let's get the price from 50 transactions ago
	var oldPrice float64
	var oldTime int64
	row = db.QueryRow("SELECT calculated_price, block_time FROM transactions ORDER BY block_time DESC, slot DESC LIMIT 1 OFFSET 50")
	err = row.Scan(&oldPrice, &oldTime)
	if err == nil {
		latestTimeRow := db.QueryRow("SELECT block_time FROM transactions ORDER BY block_time DESC, slot DESC LIMIT 1")
		var latestTime int64
		latestTimeRow.Scan(&latestTime)
		
		timeDiffDays := float64(latestTime - oldTime) / 86400.0
		if timeDiffDays > 0 {
			stats.Velocity24h = (stats.Price - oldPrice) / timeDiffDays
		}
	} else {
		// Fallback if less than 50 transactions
		stats.Velocity24h = (stats.Price - 1.0) / (30.0) // assume 30 days since launch
	}

	// Acceleration factor (velocity of last 10 txs vs last 100 txs)
	var price10 float64
	var time10 int64
	db.QueryRow("SELECT calculated_price, block_time FROM transactions ORDER BY block_time DESC, slot DESC LIMIT 1 OFFSET 10").Scan(&price10, &time10)
	var price100 float64
	var time100 int64
	db.QueryRow("SELECT calculated_price, block_time FROM transactions ORDER BY block_time DESC, slot DESC LIMIT 1 OFFSET 100").Scan(&price100, &time100)

	latestTimeRow := db.QueryRow("SELECT block_time FROM transactions ORDER BY block_time DESC, slot DESC LIMIT 1")
	var latestTime int64
	latestTimeRow.Scan(&latestTime)

	if time10 > 0 && time100 > 0 && latestTime > time10 && time10 > time100 {
		velRecent := (stats.Price - price10) / (float64(latestTime-time10) / 86400.0)
		velHistoric := (price10 - price100) / (float64(time10-time100) / 86400.0)
		if velHistoric > 0 {
			stats.AccelerationFactor = velRecent / velHistoric
		} else {
			stats.AccelerationFactor = 1.0
		}
	} else {
		stats.AccelerationFactor = 1.0
	}

	json.NewEncoder(w).Encode(stats)
}

// handleSync triggers a manual sync check
func handleSync(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	syncMu.Lock()
	if syncing {
		syncMu.Unlock()
		w.Write([]byte(`{"success":false,"message":"Sync is already running"}`))
		return
	}
	syncing = true
	syncMu.Unlock()

	go func() {
		defer func() {
			syncMu.Lock()
			syncing = false
			syncMu.Unlock()
		}()
		syncLatestTransactions()
	}()

	w.Write([]byte(`{"success":true,"message":"Sync started in background"}`))
}

// handleLiveStream registers SSE client for live updates
func handleLiveStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := make(chan TxDetail, 10)
	clientsMu.Lock()
	clients[ch] = true
	clientsMu.Unlock()

	defer func() {
		clientsMu.Lock()
		delete(clients, ch)
		clientsMu.Unlock()
		close(ch)
	}()

	// Heartbeat to keep connection alive
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	cn, ok := w.(http.CloseNotifier)
	var closeChan <-chan bool
	if ok {
		closeChan = cn.CloseNotify()
	}

	for {
		select {
		case tx := <-ch:
			data, err := json.Marshal(tx)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "data: %s\n\n", string(data))
			w.(http.Flusher).Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			w.(http.Flusher).Flush()
		case <-closeChan:
			return
		case <-r.Context().Done():
			return
		}
	}
}

// broadcastTx sends new transactions to all live SSE clients
func broadcastTx(tx TxDetail) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	for ch := range clients {
		select {
		case ch <- tx:
		default:
			// Client channel full, skip
		}
	}
}

// --- Historical Replay & Parse Engine ---

// TokenBalanceJSON represents the parsed token balances in transaction metadata
type TokenBalanceJSON struct {
	AccountIndex int    `json:"accountIndex"`
	Mint         string `json:"mint"`
	Owner        string `json:"owner"`
}

// RawTxJSON represents the schema we get from Solana RPC and cache file
type RawTxJSON struct {
	BlockTime int64 `json:"blockTime"`
	Slot      int64 `json:"slot"`
	Meta      struct {
		Err               interface{}        `json:"err"`
		LogMessages       []string           `json:"logMessages"`
		PostTokenBalances []TokenBalanceJSON `json:"postTokenBalances"`
		PreTokenBalances  []TokenBalanceJSON `json:"preTokenBalances"`
		InnerInsts        []struct {
			Insts []struct {
				Program   string `json:"program"`
				ProgramId string `json:"programId"`
				Parsed    struct {
					Type string `json:"type"`
					Info struct {
						Amount      string `json:"amount"`
						Mint        string `json:"mint"`
						Source      string `json:"source"`
						Destination string `json:"destination"`
						Authority   string `json:"authority"`
					} `json:"info"`
				} `json:"parsed"`
			} `json:"instructions"`
		} `json:"innerInstructions"`
	} `json:"meta"`
	Transaction struct {
		Message struct {
			AccountKeys []struct {
				Pubkey string `json:"pubkey"`
				Signer bool   `json:"signer"`
			} `json:"accountKeys"`
			Instructions []struct {
				ProgramId string `json:"programId"`
				Data      string `json:"data"`
			} `json:"instructions"`
		} `json:"message"`
	} `json:"transaction"`
}

func importRawTransactions() {
	// Search in scratch folder and local folder
	scratchDir := "/Users/bjoern/.gemini/antigravity-ide/brain/3f640407-d805-4f39-830d-ecbc7ad20aa3/scratch"
	filename := filepath.Join(scratchDir, "all_txs_raw.json")
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		filename = "all_txs_raw.json"
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			log.Println("Raw transaction cache file not found. Will start fresh from RPC.")
			return
		}
	}

	log.Printf("Reading raw transaction cache from %s...", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Failed to open cache file: %v", err)
		return
	}
	defer file.Close()

	byteValue, _ := io.ReadAll(file)
	var rawMap map[string]interface{}
	if err := json.Unmarshal(byteValue, &rawMap); err != nil {
		log.Printf("Failed to parse cache JSON: %v", err)
		return
	}

	log.Printf("Importing %d signatures from cache...", len(rawMap))

	// Re-parse into RawTxJSON struct
	var txs []struct {
		Signature string
		Tx        RawTxJSON
	}

	for sig, val := range rawMap {
		data, err := json.Marshal(val)
		if err != nil {
			continue
		}
		var rawTx RawTxJSON
		if err := json.Unmarshal(data, &rawTx); err == nil && rawTx.BlockTime > 0 {
			txs = append(txs, struct {
				Signature string
				Tx        RawTxJSON
			}{sig, rawTx})
		}
	}

	// Sort chronologically (by slot and transaction index)
	// We can sort by slot, and blockTime
	log.Println("Sorting transactions...")
	sortTxs(txs)

	log.Printf("Sorted %d valid transactions. Replaying and inserting to DB...", len(txs))

	pool := 1000000.0
	supply := 1000000.0

	// Clear DB table first
	db.Exec("DELETE FROM transactions")

	txCount := 0
	for _, wrapper := range txs {
		parsed := parseRawTx(wrapper.Signature, wrapper.Tx, &pool, &supply)
		if parsed != nil {
			insertTxToDB(*parsed)
			txCount++
		}
	}

	log.Printf("Successfully imported %d transactions. Final Price: %f (Pool: %f, Supply: %f)", txCount, pool/supply, pool, supply)
}

func sortTxs(txs []struct {
	Signature string
	Tx        RawTxJSON
}) {
	// Simple bubble sort / selection sort or standard Go sort.
	// Since 1300 elements is small, Go's standard library or custom sort is fast.
	// We sort by blockTime ascending, then slot ascending.
	for i := 0; i < len(txs)-1; i++ {
		for j := i + 1; j < len(txs); j++ {
			swap := false
			if txs[i].Tx.BlockTime > txs[j].Tx.BlockTime {
				swap = true
			} else if txs[i].Tx.BlockTime == txs[j].Tx.BlockTime {
				if txs[i].Tx.Slot > txs[j].Tx.Slot {
					swap = true
				}
			}
			if swap {
				txs[i], txs[j] = txs[j], txs[i]
			}
		}
	}
}

// decodeBase58 decodes a base58 string to a byte array.
func decodeBase58(s string) []byte {
	alphabet := "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	res := make([]byte, 0)
	for i := 0; i < len(s); i++ {
		c := strings.IndexByte(alphabet, s[i])
		if c == -1 {
			return nil
		}
		carry := int(c)
		for j := 0; j < len(res); j++ {
			carry += int(res[j]) * 58
			res[j] = byte(carry & 0xff)
			carry >>= 8
		}
		for carry > 0 {
			res = append(res, byte(carry&0xff))
			carry >>= 8
		}
	}
	// The decoded big integer bytes need to be reversed to represent the original serialized array
	// since standard Solana Base58 encodes the array as big-endian
	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	return res
}

func parseRawTx(sig string, raw RawTxJSON, pool *float64, supply *float64) *TxDetail {
	// 1. Identify Instruction Type
	var instType string
	for _, logMsg := range raw.Meta.LogMessages {
		if strings.Contains(logMsg, "Instruction: LeverageBuy") {
			instType = "LeverageBuy"
			break
		}
		if strings.Contains(logMsg, "Instruction: EarlyCloseLeverage") {
			instType = "EarlyCloseLeverage"
			break
		}
		if strings.Contains(logMsg, "Instruction: BuyAndLockToken") {
			instType = "BuyAndLockToken"
			break
		}
		if strings.Contains(logMsg, "Instruction: EarlyUnlockTokens") {
			instType = "EarlyUnlockTokens"
			break
		}
		if strings.Contains(logMsg, "Instruction: Initialize") {
			instType = "Initialize"
			break
		}
		if strings.Contains(logMsg, "Instruction: ClaimFounderShare") {
			instType = "ClaimFounderShare"
			break
		}
	}

	if instType == "" {
		return nil
	}

	// 2. Identify User Address (Signer)
	var signer string
	for _, key := range raw.Transaction.Message.AccountKeys {
		if key.Signer {
			signer = key.Pubkey
			break
		}
	}

	detail := TxDetail{
		Signature:       sig,
		BlockTime:       raw.BlockTime,
		Slot:            uint64(raw.Slot),
		InstructionType: instType,
		UserAddress:     signer,
		Leverage:        5, // default fallback
	}

	// Decode outer instruction for Leverage
	for _, inst := range raw.Transaction.Message.Instructions {
		if inst.ProgramId == programAddress {
			dataB58 := inst.Data
			if dataB58 != "" {
				data := decodeBase58(dataB58)
				if len(data) >= 24 {
					// Anchor discriminator (8 bytes), Amount (8 bytes), Leverage (8 bytes)
					lev := uint64(data[16]) | uint64(data[17])<<8 | uint64(data[18])<<16 | uint64(data[19])<<24 |
						uint64(data[20])<<32 | uint64(data[21])<<40 | uint64(data[22])<<48 | uint64(data[23])<<56
					detail.Leverage = int(lev)
				}
			}
		}
	}

	if instType == "Initialize" {
		// Starting state: $1.00 pool, 1.00 supply
		*pool = 1.0
		*supply = 1.0
		detail.USDCAmount = 1.0
		detail.NotionalAmount = 1.0
		detail.UPOnlyAmount = 1.0
		detail.CalculatedPrice = 1.0
		detail.PoolAfter = *pool
		detail.SupplyAfter = *supply
		return &detail
	}

	// Extract mints, burns, transfers from inner instructions
	var usdcTransferIn float64
	var usdcTransferOut float64
	var uponlyMinted float64
	var uponlyBurned float64

	for _, inner := range raw.Meta.InnerInsts {
		for _, inst := range inner.Insts {
			parsed := inst.Parsed
			if parsed.Type == "transfer" {
				info := parsed.Info
				amt, _ := strconv.ParseFloat(info.Amount, 64)
				amt /= 1e6 // USDC has 6 decimals

				if info.Destination == usdcVault {
					usdcTransferIn += amt
				}
				if info.Source == usdcVault {
					usdcTransferOut += amt
				}
			} else if parsed.Type == "mintTo" {
				info := parsed.Info
				if info.Mint == uponlyMint {
					amt, _ := strconv.ParseFloat(info.Amount, 64)
					amt /= 1e9 // UPONLY has 9 decimals
					uponlyMinted += amt
				}
			} else if parsed.Type == "burn" {
				info := parsed.Info
				if info.Mint == uponlyMint {
					amt, _ := strconv.ParseFloat(info.Amount, 64)
					amt /= 1e9 // UPONLY has 9 decimals
					uponlyBurned += amt
				}
			}
		}
	}

	if instType == "LeverageBuy" {
		if usdcTransferIn == 0 || uponlyMinted == 0 {
			return nil
		}
		lev := float64(detail.Leverage)
		userDeposit := usdcTransferIn / (1.0 - lev*0.0275)
		notional := userDeposit * lev
		fee := notional * 0.0275

		detail.USDCAmount = userDeposit
		detail.NotionalAmount = notional
		detail.UPOnlyAmount = uponlyMinted
		detail.Fee = fee
		// Prices are calculated dynamically in replay, keep placeholders
		detail.CalculatedPrice = *pool / *supply
		detail.PoolAfter = *pool
		detail.SupplyAfter = *supply
		return &detail
	}

	if instType == "BuyAndLockToken" {
		if usdcTransferIn == 0 || uponlyMinted == 0 {
			return nil
		}
		detail.USDCAmount = usdcTransferIn
		detail.NotionalAmount = usdcTransferIn
		detail.Leverage = 1
		detail.UPOnlyAmount = uponlyMinted
		detail.Fee = 0.0
		detail.CalculatedPrice = *pool / *supply
		detail.PoolAfter = *pool
		detail.SupplyAfter = *supply
		return &detail
	}

	if instType == "EarlyCloseLeverage" {
		if usdcTransferOut == 0 || uponlyBurned == 0 {
			return nil
		}
		detail.UPOnlyAmount = uponlyBurned
		detail.CalculatedPrice = *pool / *supply
		detail.PoolAfter = *pool
		detail.SupplyAfter = *supply

		// Look for user payout
		var userPayout float64
		for _, inner := range raw.Meta.InnerInsts {
			for _, inst := range inner.Insts {
				parsed := inst.Parsed
				if parsed.Type == "transfer" && parsed.Info.Source == usdcVault {
					destOwner := getOwnerOfAccount(parsed.Info.Destination, raw)
					if destOwner == signer || parsed.Info.Destination == signer {
						amt, _ := strconv.ParseFloat(parsed.Info.Amount, 64)
						userPayout += amt / 1e6
					}
				}
			}
		}

		if userPayout == 0 {
			userPayout = usdcTransferOut * 0.2877 // approximate fallback
		}

		detail.USDCAmount = userPayout
		detail.NotionalAmount = usdcTransferOut
		detail.Fee = usdcTransferOut * 0.0275 / 0.9275
		return &detail
	}

	if instType == "EarlyUnlockTokens" {
		if usdcTransferOut == 0 || uponlyBurned == 0 {
			return nil
		}
		detail.UPOnlyAmount = uponlyBurned
		detail.CalculatedPrice = *pool / *supply
		detail.PoolAfter = *pool
		detail.SupplyAfter = *supply

		var userPayout float64
		for _, inner := range raw.Meta.InnerInsts {
			for _, inst := range inner.Insts {
				parsed := inst.Parsed
				if parsed.Type == "transfer" && parsed.Info.Source == usdcVault {
					destOwner := getOwnerOfAccount(parsed.Info.Destination, raw)
					if destOwner == signer || parsed.Info.Destination == signer {
						amt, _ := strconv.ParseFloat(parsed.Info.Amount, 64)
						userPayout += amt / 1e6
					}
				}
			}
		}

		if userPayout == 0 {
			userPayout = usdcTransferOut * 0.90
		}

		detail.USDCAmount = userPayout
		detail.NotionalAmount = usdcTransferOut
		detail.Leverage = 1
		detail.Fee = usdcTransferOut * 0.10
		return &detail
	}

	return nil
}

func getOwnerOfAccount(acc string, raw RawTxJSON) string {
	idx := -1
	for i, key := range raw.Transaction.Message.AccountKeys {
		if key.Pubkey == acc {
			idx = i
			break
		}
	}
	if idx == -1 {
		return ""
	}
	for _, bal := range raw.Meta.PostTokenBalances {
		if bal.AccountIndex == idx {
			return bal.Owner
		}
	}
	for _, bal := range raw.Meta.PreTokenBalances {
		if bal.AccountIndex == idx {
			return bal.Owner
		}
	}
	return ""
}

func insertTxToDB(tx TxDetail) {
	stmt, err := db.Prepare(`
	INSERT OR REPLACE INTO transactions (
		signature, block_time, slot, instruction_type, user_address,
		usdc_amount, notional_amount, leverage, uponly_amount, fee,
		calculated_price, pool_after, supply_after, duration_days,
		profit_absolute, profit_percent, buy_signature
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Printf("Insert statement prepare failed: %v", err)
		return
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		tx.Signature, tx.BlockTime, tx.Slot, tx.InstructionType, tx.UserAddress,
		tx.USDCAmount, tx.NotionalAmount, tx.Leverage, tx.UPOnlyAmount, tx.Fee,
		tx.CalculatedPrice, tx.PoolAfter, tx.SupplyAfter, tx.DurationDays,
		tx.ProfitAbsolute, tx.ProfitPercent, tx.BuySignature,
	)
	if err != nil {
		log.Printf("Failed to insert transaction %s: %v", tx.Signature, err)
	}
}

// replayAllTransactions recalculates price history to ensure consistency
func replayAllTransactions() {
	log.Println("Replaying database history to verify and refresh computed prices...")

	// Open positions tracker
	type Position struct {
		Deposit   float64
		Loan      float64
		BlockTime int64
		Signature string
		Leverage  int
	}
	openPositions := make(map[string]Position)

	rows, err := db.Query("SELECT signature, instruction_type, usdc_amount, notional_amount, uponly_amount, slot, block_time, leverage FROM transactions ORDER BY block_time ASC, slot ASC")
	if err != nil {
		log.Printf("Replay query failed: %v", err)
		return
	}
	defer rows.Close()

	type FullTx struct {
		Sig      string
		InstType string
		Usdc     float64
		Notional float64
		UPO      float64
		Slot     uint64
		Bt       int64
		Lev      int
	}

	var txs []FullTx
	for rows.Next() {
		var tx FullTx
		rows.Scan(&tx.Sig, &tx.InstType, &tx.Usdc, &tx.Notional, &tx.UPO, &tx.Slot, &tx.Bt, &tx.Lev)
		txs = append(txs, tx)
	}

	pool := 1.0
	supply := 1.0
	totalLoans := 0.0

	for _, tx := range txs {
		var durationDays float64
		var profitAbsolute float64
		var profitPercent float64
		var buySignature string
		buyLeverage := tx.Lev // default to the tx's own leverage

		if tx.InstType == "Initialize" {
			pool = 1.0
			supply = 1.0
			totalLoans = 0.0
		} else if tx.InstType == "LeverageBuy" {
			lev := float64(tx.Lev)
			deposit := tx.Usdc // USDCAmount stores deposit
			vaultInflow := deposit * (1.0 - lev*0.0275)
			loan := deposit * (lev - 1)
			
			key := fmt.Sprintf("%.8f", tx.UPO)
			openPositions[key] = Position{
				Deposit:   deposit,
				Loan:      loan,
				BlockTime: tx.Bt,
				Signature: tx.Sig,
				Leverage:  tx.Lev,
			}

			pool += vaultInflow
			totalLoans += loan
			supply += tx.UPO
		} else if tx.InstType == "BuyAndLockToken" {
			deposit := tx.Usdc
			key := fmt.Sprintf("%.8f", tx.UPO)
			openPositions[key] = Position{
				Deposit:   deposit,
				Loan:      0.0,
				BlockTime: tx.Bt,
				Signature: tx.Sig,
				Leverage:  1,
			}

			pool += deposit
			supply += tx.UPO
		} else if tx.InstType == "EarlyCloseLeverage" {
			key := fmt.Sprintf("%.8f", tx.UPO)
			var deposit, loan float64
			var buyTime int64
			if pos, found := openPositions[key]; found {
				deposit = pos.Deposit
				loan = pos.Loan
				buyTime = pos.BlockTime
				buySignature = pos.Signature
				buyLeverage = pos.Leverage
				delete(openPositions, key)
			}

			profitAbsolute = tx.Usdc - deposit
			if deposit > 0 {
				profitPercent = (profitAbsolute / deposit) * 100.0
			}
			if buyTime > 0 {
				durationDays = float64(tx.Bt-buyTime) / 86400.0
			}

			pool -= tx.Notional // NotionalAmount stores vaultOutflow
			totalLoans -= loan
			supply -= tx.UPO
		} else if tx.InstType == "EarlyUnlockTokens" {
			key := fmt.Sprintf("%.8f", tx.UPO)
			var deposit float64
			var buyTime int64
			buyLeverage = 1
			if pos, found := openPositions[key]; found {
				deposit = pos.Deposit
				buyTime = pos.BlockTime
				buySignature = pos.Signature
				buyLeverage = pos.Leverage
				delete(openPositions, key)
			}

			profitAbsolute = tx.Usdc - deposit
			if deposit > 0 {
				profitPercent = (profitAbsolute / deposit) * 100.0
			}
			if buyTime > 0 {
				durationDays = float64(tx.Bt-buyTime) / 86400.0
			}

			pool -= tx.Notional // NotionalAmount stores vaultOutflow
			supply -= tx.UPO
		}

		priceAfter := (pool + totalLoans) / supply
		
		db.Exec(`UPDATE transactions SET 
			calculated_price = ?, 
			pool_after = ?, 
			supply_after = ?, 
			duration_days = ?, 
			profit_absolute = ?, 
			profit_percent = ?, 
			buy_signature = ?,
			leverage = ? 
			WHERE signature = ?`, 
			priceAfter, pool+totalLoans, supply, durationDays, profitAbsolute, profitPercent, buySignature, buyLeverage, tx.Sig)
	}
	log.Println("History replay completed.")
}

// --- Live Solana Synchronization ---

func syncLatestTransactions() {
	log.Println("Syncing latest transactions from Solana...")
	// 1. Fetch latest signatures
	sigs := fetchSignaturesFromRPC(programAddress, "")
	if len(sigs) == 0 {
		log.Println("No signatures retrieved from RPC. Sync skipped.")
		return
	}

	// 2. Identify new signatures (not in DB)
	var newSigs []string
	for _, sigInfo := range sigs {
		var exists bool
		db.QueryRow("SELECT EXISTS(SELECT 1 FROM transactions WHERE signature = ?)", sigInfo.Signature).Scan(&exists)
		if !exists {
			newSigs = append(newSigs, sigInfo.Signature)
		}
	}

	log.Printf("Found %d new transactions to sync.", len(newSigs))
	if len(newSigs) == 0 {
		return
	}

	// 3. Fetch transaction details and insert
	// We fetch chronologically (reverse the slice)
	for i := len(newSigs) - 1; i >= 0; i-- {
		sig := newSigs[i]
		log.Printf("Syncing new transaction: %s...", sig)
		rawTx := fetchTransactionDetailFromRPC(sig)
		if rawTx != nil {
			// Get current pool/supply state to apply delta
			var pool, supply float64
			db.QueryRow("SELECT pool_after, supply_after FROM transactions ORDER BY block_time DESC, slot DESC LIMIT 1").Scan(&pool, &supply)
			if pool == 0 {
				pool = 1000000.0
				supply = 1000000.0
			}

			parsed := parseRawTx(sig, *rawTx, &pool, &supply)
			if parsed != nil {
				insertTxToDB(*parsed)
				// Broadcast live event to SSE clients
				broadcastTx(*parsed)
				log.Printf("New tx synced! Type: %s, Price: %f", parsed.InstructionType, parsed.CalculatedPrice)
			}
		}
		time.Sleep(200 * time.Millisecond) // rate limit protection
	}
}

type RPCSubSignature struct {
	Signature string `json:"signature"`
	Slot      int64  `json:"slot"`
}

func fetchSignaturesFromRPC(address string, before string) []RPCSubSignature {
	params := map[string]interface{}{"limit": 100}
	if before != "" {
		params["before"] = before
	}
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getSignaturesForAddress",
		"params":  []interface{}{address, params},
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post("https://api.mainnet-beta.solana.com", "application/json", strings.NewReader(string(body)))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var res struct {
		Result []RPCSubSignature `json:"result"`
	}
	respBody, _ := io.ReadAll(resp.Body)
	json.Unmarshal(respBody, &res)
	return res.Result
}

func fetchTransactionDetailFromRPC(sig string) *RawTxJSON {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTransaction",
		"params": []interface{}{
			sig,
			map[string]interface{}{
				"encoding":                           "jsonParsed",
				"maxSupportedTransactionVersion": 0,
			},
		},
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post("https://api.mainnet-beta.solana.com", "application/json", strings.NewReader(string(body)))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var res struct {
		Result *RawTxJSON `json:"result"`
	}
	respBody, _ := io.ReadAll(resp.Body)
	json.Unmarshal(respBody, &res)
	return res.Result
}
