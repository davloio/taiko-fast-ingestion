use anyhow::Result;
use ethers::{
    providers::{Http, Middleware, Provider, Ws},
};
use futures::StreamExt;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::models::BlockData;

pub struct BlockchainClient {
    http_provider: Arc<Provider<Http>>,
    ws_provider: Option<Arc<Provider<Ws>>>,
}

impl BlockchainClient {
    pub async fn new(http_url: &str, ws_url: &str) -> Result<Self> {
        // HTTP provider (required)
        let http_provider = Provider::<Http>::try_from(http_url)?;
        info!("Connected to Taiko HTTP RPC at {}", http_url);

        // Skip WebSocket for now due to connection issues
        warn!("Skipping WebSocket connection, using HTTP polling mode only");
        let ws_provider = None;

        Ok(Self {
            http_provider: Arc::new(http_provider),
            ws_provider,
        })
    }

    pub async fn get_latest_block_number(&self) -> Result<u64> {
        let block_number = self.http_provider.get_block_number().await?;
        Ok(block_number.as_u64())
    }

    pub async fn get_block_data(&self, block_number: u64) -> Result<Option<BlockData>> {
        let block = self
            .http_provider
            .get_block_with_txs(block_number)
            .await?;

        match block {
            Some(block) => {
                let transactions: Vec<String> = block
                    .transactions
                    .iter()
                    .map(|tx| format!("{:?}", tx.hash))
                    .collect();

                Ok(Some(BlockData {
                    number: block.number.unwrap_or_default().as_u64(),
                    hash: format!("{:?}", block.hash.unwrap_or_default()),
                    timestamp: block.timestamp.as_u64(),
                    transactions,
                }))
            }
            None => {
                debug!("Block {} not found", block_number);
                Ok(None)
            }
        }
    }

    pub async fn get_block_range(&self, from: u64, to: u64) -> Result<Vec<BlockData>> {
        use futures::future::join_all;
        use std::sync::Arc;
        use tokio::sync::Semaphore;
        
        info!("Fetching blocks {} to {} with PublicNode RPC (higher limits)", from, to);
        
        // Use moderate concurrency since PublicNode has better rate limits
        let semaphore = Arc::new(Semaphore::new(20)); // 20 concurrent requests
        let mut futures = Vec::new();
        
        for block_number in from..=to {
            let provider = Arc::clone(&self.http_provider);
            let permit = Arc::clone(&semaphore);
            
            let future = async move {
                let _permit = permit.acquire().await.unwrap();
                
                let mut retries = 0;
                let max_retries = 2;
                
                loop {
                    match provider.get_block_with_txs(block_number).await {
                        Ok(Some(block)) => {
                            let transactions: Vec<String> = block
                                .transactions
                                .iter()
                                .map(|tx| format!("{:?}", tx.hash))
                                .collect();

                            return Some(BlockData {
                                number: block.number.unwrap_or_default().as_u64(),
                                hash: format!("{:?}", block.hash.unwrap_or_default()),
                                timestamp: block.timestamp.as_u64(),
                                transactions,
                            });
                        }
                        Ok(None) => {
                            debug!("Block {} not found", block_number);
                            return None;
                        }
                        Err(e) => {
                            if e.to_string().contains("429") && retries < max_retries {
                                retries += 1;
                                let delay = 1000 * retries as u64; // 1s, 2s backoff
                                tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                            } else {
                                error!("Failed to fetch block {}: {}", block_number, e);
                                return None;
                            }
                        }
                    }
                }
            };
            
            futures.push(future);
        }
        
        let results = join_all(futures).await;
        let blocks: Vec<BlockData> = results.into_iter().flatten().collect();
        
        info!("Successfully fetched {} blocks concurrently", blocks.len());
        Ok(blocks)
    }

    pub async fn subscribe_and_process_blocks(&self, db: Arc<crate::db::Database>) -> Result<()> {
        if let Some(ws_provider) = &self.ws_provider {
            info!("Starting block subscription...");
            
            let mut stream = ws_provider.subscribe_blocks().await?;
            
            while let Some(block) = stream.next().await {
                let block_data = BlockData {
                    number: block.number.unwrap_or_default().as_u64(),
                    hash: format!("{:?}", block.hash.unwrap_or_default()),
                    timestamp: block.timestamp.as_u64(),
                    transactions: block
                        .transactions
                        .iter()
                        .map(|hash| format!("{:?}", hash))
                        .collect(),
                };
                
                debug!("Received new block: {}", block_data.number);
                
                if let Err(e) = crate::ingestion::process_single_block(&db, block_data).await {
                    error!("Failed to process live block: {}", e);
                }
            }
            
            Ok(())
        } else {
            Err(anyhow::anyhow!("WebSocket connection required for live mode"))
        }
    }

    pub fn has_websocket(&self) -> bool {
        self.ws_provider.is_some()
    }
}