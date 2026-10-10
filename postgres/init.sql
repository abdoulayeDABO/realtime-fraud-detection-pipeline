CREATE TABLE IF NOT EXISTS transactions_scored (
    transaction_id  TEXT PRIMARY KEY,
    tx_time         DOUBLE PRECISION,
    amount          DOUBLE PRECISION,
    fraud_score     DOUBLE PRECISION,
    is_fraud        INT,
    ts              TIMESTAMPTZ NOT NULL DEFAULT now() 
);

CREATE INDEX IF NOT EXISTS idx_transactions_scored_ts ON transactions_scored (ts);