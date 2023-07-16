CREATE TABLE IF NOT EXISTS rates (
	unix BIGINT NOT NULL PRIMARY KEY,
	rate_date TIMESTAMP NOT NULL,
	symbol VARCHAR(10) NOT NULL,
	price_open DECIMAL(20, 2) NOT NULL,
	price_close DECIMAL(20, 2) NOT NULL,
	price_high DECIMAL(20, 2) NOT NULL,
	price_low DECIMAL(20, 2) NOT NULL,
	volume_eth DECIMAL(20, 2) NOT NULL,
	volume_usd DECIMAL(20, 2) NOT NULL
);
