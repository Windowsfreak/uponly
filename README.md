# UPOnly Tracker & Analytics Dashboard

A high-performance Golang backend caching service and visual explorer dashboard designed to track the historical price appreciation of the UPOnly token (`Hm6QaYdnze5BQGgzbLYeLr5yjYJnKZ7tKvbwi44ebogd`) on Solana Mainnet-Beta.

Calculations are verified directly against on-chain transactions, reproducing the live circulating supply, liquidity pool size, and token price to the 14th decimal place.

## Features

* **Unified On-Chain Mathematics**: Replays and models Solana Mainnet transactions chronologically starting from genesis to calculate token price. Correctly tracks outstanding leverage debt (`Pool = Cash_Vault + Outstanding_Loans`) and circulating supply.
* **Anchor Instruction Decoder**: Decodes transaction instruction logs to unpack leverage values (2x, 3x, 4x, 5x) selected for each position, ensuring 100% precise loan calculations.
* **Trader Hold Durations & ROI**: Matches close/unlock transactions to their corresponding buy/locks to compute hold duration (in days/hours/minutes) and net USDC cash-flow returns (ROI %).
* **Dynamic Analytics Panel**:
  * **Price & Vol**: Opaque price line overlaid with daily net buy (green) or sell (red) surplus volume bars.
  * **Pool Assets**: Visualizes the pool's asset structure by overlapping Total Pool, Vault Cash, and outstanding Leverage loans.
  * **Buys/Sells Vol**: Tracks cumulative USDC spent buying vs. USDC taken out on selling.
  * **Break-Even Hold**: Plots the break-even holding period required to profit on a given day based on the $P(\text{buy}) \le 0.81 \times P(\text{sell})$ threshold.
* **Dual X-Axis Scaling**: Switches the chart X-axis between **TX Index** (transaction-level resolution showing all 900+ checkpoints) and **Temporal** (daily close aggregations).
* **Predictive Simulator**: Simulates buy/sell transaction scenarios using the correct pool growth formula. Because both buys and sells generate fees, the token price always appreciates. Sliders use logarithmic index steps (up to 500M) for easy control.
* **High-Speed CSV Export**: Downloads the entire transaction list history as a clean CSV file in one click.
* **Live SSE Updates**: Streams real-time blockchain updates to the client using Server-Sent Events (SSE).

---

## Technical Stack

* **Backend**: Go (Golang)
* **Database**: SQLite3 (used for fast chronological transaction caching)
* **Frontend**: Pure HTML, CSS (Vanilla), Javascript (Chart.js for visualizations)

---

## Getting Started

### Prerequisites

* Go 1.18 or higher installed on your computer.

### Build and Run

1. Clone or navigate to the project directory:
   ```bash
   cd uponly
   ```

2. Compile the Go backend:
   ```bash
   go build -o uponly
   ```

3. Run the compiled binary:
   ```bash
   ./uponly
   ```
   On first startup, the service will automatically create the database schema in `uponly.db`, import the historical transaction cache, and perform a full replay validation.

4. Open the visual explorer dashboard in your web browser:
   ```
   http://localhost:8080
   ```
