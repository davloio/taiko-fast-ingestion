use anyhow::Result;
use chrono::Utc;
use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::{info, debug};

use crate::models::{IngestionState, BlockData};

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        
        info!("Connected to database with {} max connections", max_connections);
        
        // Run migrations
        Self::run_migrations(&pool).await?;
        
        Ok(Self { pool })
    }

    async fn run_migrations(pool: &PgPool) -> Result<()> {
        info!("Running database migrations...");
        
        // Create blocks table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS blocks (
                number BIGINT PRIMARY KEY,
                hash VARCHAR(66) NOT NULL UNIQUE,
                timestamp TIMESTAMPTZ NOT NULL,
                transaction_count INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )"
        ).execute(pool).await?;

        // Create transactions table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS transactions (
                hash VARCHAR(66) PRIMARY KEY,
                block_number BIGINT NOT NULL REFERENCES blocks(number),
                position INTEGER NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )"
        ).execute(pool).await?;

        // Create ingestion state table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ingestion_state (
                id INTEGER PRIMARY KEY DEFAULT 1,
                last_processed_block BIGINT DEFAULT 0,
                mode VARCHAR(20) DEFAULT 'reindex',
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )"
        ).execute(pool).await?;

        // Create indexes for optimal performance
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_blocks_number ON blocks(number)").execute(pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp)").execute(pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_blocks_hash ON blocks(hash)").execute(pool).await?;
        
        // Critical transaction indexes for fast ingestion
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_hash ON transactions(hash)").execute(pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_block_number ON transactions(block_number)").execute(pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)").execute(pool).await?;
        
        // Composite indexes for performance
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_block_pos ON transactions(block_number, position)").execute(pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_block_hash ON transactions(block_number, hash)").execute(pool).await?;
        
        // Blockchain explorer performance indexes
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_blocks_timestamp_desc ON blocks(timestamp DESC)").execute(pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_created_desc ON transactions(created_at DESC)").execute(pool).await?;
        
        // Partial indexes for recent data
        // Note: These use fixed values for performance. You may want to periodically recreate them
        // to adjust the boundaries as your data grows
        
        // Get approximate recent block number for partial index
        let recent_block: Option<i64> = sqlx::query_scalar(
            "SELECT COALESCE(MAX(number) - 100000, 0) FROM blocks"
        )
        .fetch_one(pool)
        .await
        .unwrap_or(Some(0));
        
        if let Some(min_block) = recent_block {
            let query = format!(
                "CREATE INDEX IF NOT EXISTS idx_blocks_recent ON blocks(number DESC) WHERE number > {}",
                min_block
            );
            sqlx::query(&query).execute(pool).await?;
        }
        
        // Partial index for recent transactions
        // We use a fixed date since NOW() is not immutable
        // Consider recreating this index periodically with a new date
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_recent ON transactions(created_at DESC) WHERE created_at > '2024-11-01'::timestamptz").execute(pool).await?;

        // Initialize ingestion state if not exists (start from block 1)
        sqlx::query(
            "INSERT INTO ingestion_state (id, last_processed_block, mode) 
             VALUES (1, 0, 'reindex') 
             ON CONFLICT (id) DO NOTHING"
        ).execute(pool).await?;

        info!("Database migrations completed");
        Ok(())
    }

    pub async fn get_ingestion_state(&self) -> Result<IngestionState> {
        let state = sqlx::query_as::<_, IngestionState>(
            "SELECT id, last_processed_block, mode, updated_at FROM ingestion_state WHERE id = 1"
        )
        .fetch_one(&self.pool)
        .await?;
        
        Ok(state)
    }

    pub async fn update_last_processed_block(&self, block_number: i64) -> Result<()> {
        sqlx::query(
            "UPDATE ingestion_state SET last_processed_block = $1, updated_at = NOW() WHERE id = 1"
        )
        .bind(block_number)
        .execute(&self.pool)
        .await?;
        
        debug!("Updated last processed block to {}", block_number);
        Ok(())
    }

    pub async fn insert_block(&self, block_data: &BlockData) -> Result<()> {
        let timestamp = chrono::DateTime::<Utc>::from_timestamp(block_data.timestamp as i64, 0)
            .unwrap_or_else(|| Utc::now());
            
        sqlx::query(
            "INSERT INTO blocks (number, hash, timestamp, transaction_count) 
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (number) DO NOTHING"
        )
        .bind(block_data.number as i64)
        .bind(&block_data.hash)
        .bind(timestamp)
        .bind(block_data.transactions.len() as i32)
        .execute(&self.pool)
        .await?;
        
        debug!("Inserted block {} with {} transactions", block_data.number, block_data.transactions.len());
        Ok(())
    }

    pub async fn insert_transactions(&self, block_number: u64, transactions: &[String]) -> Result<()> {
        if transactions.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        
        for (position, hash) in transactions.iter().enumerate() {
            sqlx::query(
                "INSERT INTO transactions (hash, block_number, position) 
                 VALUES ($1, $2, $3)
                 ON CONFLICT (hash) DO NOTHING"
            )
            .bind(hash)
            .bind(block_number as i64)
            .bind(position as i32)
            .execute(&mut *tx)
            .await?;
        }
        
        tx.commit().await?;
        debug!("Inserted {} transactions for block {}", transactions.len(), block_number);
        Ok(())
    }

    pub async fn get_latest_block_number(&self) -> Result<Option<i64>> {
        let result = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(number) FROM blocks"
        )
        .fetch_one(&self.pool)
        .await?;
        
        Ok(result)
    }

    pub async fn batch_insert_blocks(&self, blocks: &[BlockData]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        
        // Use COPY for maximum performance with large datasets
        // First, prepare blocks data
        let block_rows: Vec<_> = blocks.iter().map(|block_data| {
            let timestamp = chrono::DateTime::<Utc>::from_timestamp(block_data.timestamp as i64, 0)
                .unwrap_or_else(|| Utc::now());
            (
                block_data.number as i64,
                block_data.hash.as_str(),
                timestamp,
                block_data.transactions.len() as i32
            )
        }).collect();
        
        // Batch insert blocks using unnest for better performance
        if !block_rows.is_empty() {
            let block_numbers: Vec<i64> = block_rows.iter().map(|r| r.0).collect();
            let block_hashes: Vec<&str> = block_rows.iter().map(|r| r.1).collect();
            let block_timestamps: Vec<chrono::DateTime<Utc>> = block_rows.iter().map(|r| r.2).collect();
            let block_tx_counts: Vec<i32> = block_rows.iter().map(|r| r.3).collect();
            
            sqlx::query(
                "INSERT INTO blocks (number, hash, timestamp, transaction_count)
                 SELECT * FROM UNNEST($1::BIGINT[], $2::VARCHAR[], $3::TIMESTAMPTZ[], $4::INTEGER[])
                 ON CONFLICT (number) DO NOTHING"
            )
            .bind(&block_numbers)
            .bind(&block_hashes)
            .bind(&block_timestamps)
            .bind(&block_tx_counts)
            .execute(&mut *tx)
            .await?;
        }
        
        // Prepare all transactions for bulk insert
        let mut all_tx_hashes = Vec::new();
        let mut all_block_numbers = Vec::new();
        let mut all_positions = Vec::new();
        
        for block_data in blocks {
            for (position, hash) in block_data.transactions.iter().enumerate() {
                all_tx_hashes.push(hash.as_str());
                all_block_numbers.push(block_data.number as i64);
                all_positions.push(position as i32);
            }
        }
        
        // Batch insert transactions using unnest
        if !all_tx_hashes.is_empty() {
            sqlx::query(
                "INSERT INTO transactions (hash, block_number, position)
                 SELECT * FROM UNNEST($1::VARCHAR[], $2::BIGINT[], $3::INTEGER[])
                 ON CONFLICT (hash) DO NOTHING"
            )
            .bind(&all_tx_hashes)
            .bind(&all_block_numbers)
            .bind(&all_positions)
            .execute(&mut *tx)
            .await?;
        }
        
        tx.commit().await?;
        debug!("Bulk inserted {} blocks with {} transactions using UNNEST", blocks.len(), all_tx_hashes.len());
        Ok(())
    }
}