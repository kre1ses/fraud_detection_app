CREATE TABLE IF NOT EXISTS scoring_results (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) NOT NULL UNIQUE,
    score DECIMAL(10, 8) NOT NULL,
    fraud_flag BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fraud_flag ON scoring_results(fraud_flag);
CREATE INDEX IF NOT EXISTS idx_created_at ON scoring_results(created_at);
CREATE INDEX IF NOT EXISTS idx_transaction_id ON scoring_results(transaction_id);

CREATE OR REPLACE VIEW recent_fraud_transactions AS
SELECT transaction_id, score, created_at
FROM scoring_results 
WHERE fraud_flag = true 
ORDER BY created_at DESC 
LIMIT 10;