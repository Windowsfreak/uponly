UPOnly Token Information

UPOnly is a token on the Solana blockchain that, when purchased or sold gets minted or burned and the price paid is stored in an inaccessible liquidity pool.

The value of this token is always liquidity pool divided by number of tokens in circulation.
For any purchase, a 10% fee is deducted, i.e. if the price is 1.00 USDC and someone buys for 1000 USDC, they will receive 909.09 tokens. For any sale, a 10% fee is also deducted.

The purpose of this project is to simulate the token value appreciation, since the above formula dictates that the price can only go up.

The question to be solved is: How many tokens need to be purchased and/or sold for the price to appreciate by a certain percentage or factor, i.e. 20% price increase (net zero), 100% price increase (I think it's something between 60-70% win).

To help with the simulation, the following values need to be provided:
- Liquidity Pool size (in USDC)
- Circulating Supply (in UP)
The price (in USDC) can already be calculated from this.
For the simulation, we also ask the user to enter:
- Target Price (in USDC) (or alternatively, target price increase factor)
In the simulation, we assume 75% buy and 25% sell transactions, but we also let the user to adjust this ratio in steps of 5%.

We start with the initial values and then simulate transactions one by one until the target price is reached. For each transaction, we calculate the new price and the new circulating supply. We also keep track of the total amount of USDC spent on buys and the total amount of USDC received from sells.

We don't show the simulation steps (we don't know how much memory that would eat), but the total number of tokens purchased and sold, the total number of USDC spent on buying and selling.