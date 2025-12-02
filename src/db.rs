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

        // Create indexes
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_blocks_number ON blocks(number)").execute(pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp)").execute(pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_block_number ON transactions(block_number)").execute(pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)").execute(pool).await?;

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
        
        // Batch insert blocks
        for block_data in blocks {
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
            .execute(&mut *tx)
            .await?;
        }
        
        // Batch insert all transactions
        for block_data in blocks {
            for (position, hash) in block_data.transactions.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO transactions (hash, block_number, position) 
                     VALUES ($1, $2, $3)
                     ON CONFLICT (hash) DO NOTHING"
                )
                .bind(hash)
                .bind(block_data.number as i64)
                .bind(position as i32)
                .execute(&mut *tx)
                .await?;
            }
        }
        
        tx.commit().await?;
        debug!("Batch inserted {} blocks with transactions", blocks.len());
        Ok(())
    }
}