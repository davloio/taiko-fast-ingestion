use anyhow::Result;
use ethers::{
    providers::{Http, Middleware, Provider, Ws},
    types::Block,
};
use futures::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::models::BlockData;

pub struct BlockchainClient {
    http_provider: Arc<Provider<Http>>,
    ws_provider: Option<Arc<Provider<Ws>>>,
    http_client: Client,
    rpc_url: String,
}

#[derive(Serialize)]
struct RpcRequest {
    jsonrpc: String,
    method: String,
    params: Vec<Value>,
    id: u64,
}

#[derive(Deserialize)]
struct RpcResponse {
    jsonrpc: String,
    result: Option<Value>,
    error: Option<Value>,
    id: u64,
}

impl BlockchainClient {
    pub async fn new(http_url: &str, _ws_url: &str) -> Result<Self> {
        // HTTP provider (required)
        let http_provider = Provider::<Http>::try_from(http_url)?;
        info!("Connected to Taiko HTTP RPC at {}", http_url);

        // Skip WebSocket for now due to connection issues
        warn!("Skipping WebSocket connection, using HTTP polling mode only");
        let ws_provider = None;

        let http_client = Client::new();

        Ok(Self {
            http_provider: Arc::new(http_provider),
            ws_provider,
            http_client,
            rpc_url: http_url.to_string(),
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
        info!("Fetching blocks {} to {} using BATCHED RPC requests", from, to);
        
        let batch_size = (to - from + 1).min(1000) as usize; // Use full range up to 1000 blocks per batch
        let mut all_blocks = Vec::new();
        
        for batch_start in (from..=to).step_by(batch_size) {
            let batch_end = (batch_start + batch_size as u64 - 1).min(to);
            
            match self.get_blocks_batch(batch_start, batch_end).await {
                Ok(mut blocks) => {
                    info!("Successfully fetched {} blocks in single batch request ({}-{})", 
                          blocks.len(), batch_start, batch_end);
                    all_blocks.append(&mut blocks);
                }
                Err(e) => {
                    error!("Batch request failed for blocks {}-{}, falling back to individual requests: {}", 
                           batch_start, batch_end, e);
                    
                    // Fallback to individual requests
                    for block_num in batch_start..=batch_end {
                        if let Ok(Some(block)) = self.get_block_data(block_num).await {
                            all_blocks.push(block);
                        }
                    }
                }
            }
            
            // Small delay between batches to avoid rate limits
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        
        info!("Successfully fetched {} total blocks using batched requests", all_blocks.len());
        Ok(all_blocks)
    }

    async fn get_blocks_batch(&self, from: u64, to: u64) -> Result<Vec<BlockData>> {
        let mut requests = Vec::new();
        
        // Create batch RPC request for multiple blocks
        for (i, block_number) in (from..=to).enumerate() {
            requests.push(RpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "eth_getBlockByNumber".to_string(),
                params: vec![
                    json!(format!("0x{:x}", block_number)),
                    json!(true) // Include transactions
                ],
                id: i as u64,
            });
        }
        
        // Send batch request
        let response = self.http_client
            .post(&self.rpc_url)
            .json(&requests)
            .send()
            .await?;
            
        if response.status().as_u16() == 429 {
            return Err(anyhow::anyhow!("Rate limited"));
        }
        
        let responses: Vec<RpcResponse> = response.json().await?;
        let mut blocks = Vec::new();
        
        for resp in responses {
            if let Some(result) = resp.result {
                if let Ok(block) = self.parse_block_data(&result) {
                    blocks.push(block);
                }
            }
        }
        
        Ok(blocks)
    }
    
    fn parse_block_data(&self, block_json: &Value) -> Result<BlockData> {
        let number_hex = block_json["number"].as_str().ok_or_else(|| anyhow::anyhow!("No block number"))?;
        let number = u64::from_str_radix(&number_hex[2..], 16)?;
        
        let hash = block_json["hash"].as_str().unwrap_or("").to_string();
        
        let timestamp_hex = block_json["timestamp"].as_str().unwrap_or("0x0");
        let timestamp = u64::from_str_radix(&timestamp_hex[2..], 16).unwrap_or(0);
        
        let transactions = if let Some(txs) = block_json["transactions"].as_array() {
            txs.iter()
                .filter_map(|tx| tx["hash"].as_str())
                .map(|hash| hash.to_string())
                .collect()
        } else {
            Vec::new()
        };
        
        Ok(BlockData {
            number,
            hash,
            timestamp,
            transactions,
        })
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