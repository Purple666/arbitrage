# Identifying triangular arbitrages in the altcoin market

The bitcoin and altcoin markets have a capitalization of over $200 billion<sup>[1](#capitalization)</sup>, with bitcoin alone reaching over $22 billion dollars worth of transaction each day<sup>[2](#volume)</sup>. Even a small inefficiency in the market can mean a lot of money available as arbitrage opportunities.

For my Insight project, I am using bitcoin exchange data to develop a dashbord that clarifies/detects triangular arbitrage opportunities so that traders can know what trades are potentially profitable.

To do this, I had to detect complex relationships among a large number of streams of data.

## Setting it up

## Running

## Data source

The data for this project from [kaiko.com](https://www.kaiko.com/), downloaded using their API during their free trial week.

Kaiko has kindly allowed access to 3 months worth of historical data (June 16th, 2019 to September 20th, 2019). Data regarding two exchanges ([Bittrex](https://bittrex.com/) and [BitFinance](https://www.bitfinance.com/) during this time period were collected.



## Architecture


## Engineering challenges


<a name="capitalization">1</a>: According to [CoinMarketCap](https://coinmarketcap.com/), on September 25th, bitcoin & altcoins 2019 together have a capitalization over $225 billion.

<a name="volume">2</a>: Also, according to [CoinMarketCap](https://coinmarketcap.com/), on September 25th, bitcoin alone had a 24 hour volume of over $22 billion.