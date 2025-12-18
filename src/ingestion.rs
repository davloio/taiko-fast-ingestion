use anyhow::Result;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::db::Database;
use crate::models::{BlockData, IngestionMode};
use crate::rpc::BlockchainClient;

pub struct IngestionService {
    db: Arc<Database>,
    blockchain_client: Arc<BlockchainClient>,
    batch_size: usize,
}

impl IngestionService {
    pub fn new(
        db: Arc<Database>,
        blockchain_client: Arc<BlockchainClient>,
        batch_size: usize,
    ) -> Self {
        Self {
            db,
            blockchain_client,
            batch_size,
        }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting ingestion service...");

        // Get current state
        let state = self.db.get_ingestion_state().await?;
        let mode: IngestionMode = state.mode.into();
        
        info!("Current mode: {:?}, last processed block: {}", mode, state.last_processed_block);

        match mode {
            IngestionMode::Reindex => {
                info!("Starting in reindex mode");
                self.run_reindex_mode(state.last_processed_block as u64).await?;
            }
            IngestionMode::Live => {
                info!("Starting in live mode");
                self.run_live_mode().await?;
            }
        }

        Ok(())
    }

    async fn run_reindex_mode(&self, start_block: u64) -> Result<()> {
        let latest_block = self.blockchain_client.get_latest_block_number().await?;
        info!("Latest block on chain: {}", latest_block);
        
        if start_block >= latest_block {
            info!("Already caught up! Switching to live mode...");
            return self.run_live_mode().await;
        }

        let mut current_block = start_block + 1;
        
        while current_block < latest_block {
            let end_block = (current_block + self.batch_size as u64 - 1).min(latest_block);
            
            info!("Processing blocks {} to {} ({} remaining)", 
                  current_block, end_block, latest_block - current_block);
            
            match self.process_block_range(current_block, end_block).await {
                Ok(Some(last_block_processed)) => {
                    self.db.update_last_processed_block(last_block_processed as i64).await?;
                    current_block = last_block_processed + 1;
                }
                Ok(None) => {
                    warn!("No blocks processed in range {} to {}", current_block, end_block);
                    sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    error!("Failed to process block range {} to {}: {}", current_block, end_block, e);
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }

        info!("Reindex complete! Switching to live mode...");
        self.run_live_mode().await
    }

    async fn run_live_mode(&self) -> Result<()> {
        if !self.blockchain_client.has_websocket() {
            warn!("No WebSocket connection available, using polling mode");
            return self.run_polling_mode().await;
        }

        info!("Subscribing to new blocks via WebSocket...");
        self.blockchain_client.subscribe_and_process_blocks(Arc::clone(&self.db)).await?;

        Ok(())
    }

    async fn run_polling_mode(&self) -> Result<()> {
        let mut last_processed = self.db.get_ingestion_state().await?.last_processed_block as u64;
        
        info!("Starting polling mode, checking every 2 seconds");
        
        loop {
            match self.blockchain_client.get_latest_block_number().await {
                Ok(latest_block) => {
                    if latest_block > last_processed {
                        info!("New blocks detected: {} to {}", last_processed + 1, latest_block);
                        
                        match self.process_block_range(last_processed + 1, latest_block).await {
                            Ok(Some(actual_last_block)) => {
                                last_processed = actual_last_block;
                                self.db.update_last_processed_block(actual_last_block as i64).await?;
                            }
                            Ok(None) => {
                                // No blocks processed, keep same last_processed
                            }
                            Err(e) => {
                                error!("Failed to process new blocks: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to get latest block number: {}", e);
                }
            }
            
            sleep(Duration::from_secs(2)).await;
        }
    }

    async fn process_block_range(&self, from: u64, to: u64) -> Result<Option<u64>> {
        let blocks = self.blockchain_client.get_block_range(from, to).await?;
        let processed_count = blocks.len();
        let last_processed_block = blocks.last().map(|b| b.number);
        
        if !blocks.is_empty() {
            // Use batch insert for much better performance
            self.db.batch_insert_blocks(&blocks).await?;
            
            let total_transactions: usize = blocks.iter().map(|b| b.transactions.len()).sum();
            info!("Batch processed {} blocks ({} transactions) from {} to {}", 
                  processed_count, total_transactions, from, to);
        }
        
        Ok(last_processed_block)
    }

    async fn process_single_block_sync(&self, block_data: &BlockData) -> Result<()> {
        // Insert block
        self.db.insert_block(block_data).await?;
        
        // Insert transactions
        self.db.insert_transactions(block_data.number, &block_data.transactions).await?;
        
        info!("Block {} processed: {} transactions", block_data.number, block_data.transactions.len());
        Ok(())
    }
}

pub async fn process_single_block(db: &Arc<Database>, block_data: BlockData) -> Result<()> {
    // Insert block
    db.insert_block(&block_data).await?;
    
    // Insert transactions
    db.insert_transactions(block_data.number, &block_data.transactions).await?;
    
    // Update last processed block
    db.update_last_processed_block(block_data.number as i64).await?;
    
    info!("Live block {} processed: {} transactions", block_data.number, block_data.transactions.len());
    Ok(())
}