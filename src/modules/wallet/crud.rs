use crate::modules::wallet::model::{PayoutAssetMetadata, PayoutFeeQuote, SwapAddressInfo};
use sqlx::{MySql, Pool, Row};

#[derive(Debug, Clone, PartialEq)]
pub enum PayoutLockResult {
    Acquired,
    AlreadyCompleted { tx_hash: String, payout_amount: f64 },
    InProgress,
}

#[derive(Clone)]
pub struct WalletCrud {
    pool: Pool<MySql>,
}

impl WalletCrud {
    pub fn new(pool: Pool<MySql>) -> Self {
        Self { pool }
    }

    /// Get the next available address index atomically using a database sequence
    /// This prevents race conditions when multiple swaps are created simultaneously
    pub async fn get_next_index(&self) -> Result<u32, sqlx::Error> {
        // Use a transaction with SELECT FOR UPDATE to lock the row
        let mut tx = self.pool.begin().await?;

        // Lock the row and get current value
        let current: (u32,) =
            sqlx::query_as("SELECT next_index FROM address_index_counter WHERE id = 1 FOR UPDATE")
                .fetch_one(&mut *tx)
                .await?;

        let next_index = current.0 + 1;

        // Update to the new value
        sqlx::query("UPDATE address_index_counter SET next_index = ? WHERE id = 1")
            .bind(next_index)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(next_index)
    }

    /// Save address information for a swap
    pub async fn save_address_info(
        &self,
        swap_id: &str,
        our_address: &str,
        address_index: u32,
        coin_type: i32,
        user_recipient_address: &str,
        user_recipient_extra_id: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO swap_address_info (
                swap_id, our_address, address_index, blockchain_id,
                coin_type, recipient_address, recipient_extra_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(swap_id)
        .bind(our_address)
        .bind(address_index)
        .bind(1) // Default blockchain_id for now
        .bind(coin_type)
        .bind(user_recipient_address)
        .bind(user_recipient_extra_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Fetch address info for a specific swap
    pub async fn get_address_info(
        &self,
        swap_id: &str,
    ) -> Result<Option<SwapAddressInfo>, sqlx::Error> {
        sqlx::query_as::<_, SwapAddressInfo>(
            r#"
            SELECT
                sa.swap_id,
                sa.our_address,
                sa.address_index,
                sa.blockchain_id,
                sa.coin_type,
                s.to_currency AS payout_ticker,
                s.to_network AS payout_network,
                sa.recipient_address,
                sa.recipient_extra_id,
                sa.commission_rate,
                sa.payout_tx_hash,
                sa.payout_amount,
                sa.actual_received,
                sa.commission_taken,
                sa.network_fee_paid,
                sa.status,
                sa.created_at,
                sa.signed_at,
                sa.broadcast_at,
                sa.confirmed_at,
                sa.last_balance_check
            FROM swap_address_info sa
            JOIN swaps s ON s.id = sa.swap_id
            WHERE sa.swap_id = ?
            "#,
        )
        .bind(swap_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_payout_fee_quote(
        &self,
        swap_id: &str,
    ) -> Result<Option<PayoutFeeQuote>, sqlx::Error> {
        sqlx::query_as::<_, PayoutFeeQuote>(
            r#"
            SELECT CAST(platform_fee AS DOUBLE) AS platform_fee
            FROM swaps
            WHERE id = ?
            "#,
        )
        .bind(swap_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_payout_asset_metadata(
        &self,
        symbol: &str,
        requested_network: &str,
        lookup_network: &str,
    ) -> Result<Option<PayoutAssetMetadata>, sqlx::Error> {
        let token_row = sqlx::query(
            r#"
            SELECT
                symbol,
                network,
                contract_address,
                decimals,
                CAST(gas_multiplier AS DOUBLE) AS gas_multiplier
            FROM tokens
            WHERE UPPER(symbol) = UPPER(?)
              AND LOWER(network) = LOWER(?)
              AND is_active = TRUE
            ORDER BY is_verified DESC, contract_address IS NULL ASC, id DESC
            LIMIT 1
            "#,
        )
        .bind(symbol)
        .bind(lookup_network)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = token_row {
            return Ok(Some(Self::row_to_payout_asset_metadata(&row)));
        }

        let currency_row = sqlx::query(
            r#"
            SELECT
                symbol,
                network,
                contract_address,
                decimals
            FROM currencies
            WHERE UPPER(symbol) = UPPER(?)
              AND is_active = TRUE
              AND (
                  LOWER(network) = LOWER(?)
                  OR LOWER(network) = LOWER(?)
              )
            ORDER BY CASE
                WHEN LOWER(network) = LOWER(?) THEN 0
                ELSE 1
            END,
            contract_address IS NULL ASC,
            id DESC
            LIMIT 1
            "#,
        )
        .bind(symbol)
        .bind(requested_network)
        .bind(lookup_network)
        .bind(requested_network)
        .fetch_optional(&self.pool)
        .await?;

        Ok(currency_row
            .as_ref()
            .map(Self::row_to_payout_asset_metadata_from_currency))
    }

    pub async fn acquire_payout_lock(
        &self,
        swap_id: &str,
    ) -> Result<PayoutLockResult, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE swap_address_info
            SET status = 'processing',
                signed_at = NOW()
            WHERE swap_id = ?
              AND payout_tx_hash IS NULL
              AND status IN ('pending', 'failed')
            "#,
        )
        .bind(swap_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 1 {
            return Ok(PayoutLockResult::Acquired);
        }

        let row = sqlx::query(
            r#"
            SELECT payout_tx_hash, payout_amount, status
            FROM swap_address_info
            WHERE swap_id = ?
            "#,
        )
        .bind(swap_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Err(sqlx::Error::RowNotFound);
        };

        if let Some(tx_hash) = row.try_get::<Option<String>, _>("payout_tx_hash")? {
            return Ok(PayoutLockResult::AlreadyCompleted {
                tx_hash,
                payout_amount: row
                    .try_get::<Option<f64>, _>("payout_amount")?
                    .unwrap_or(0.0),
            });
        }

        let status = row.try_get::<String, _>("status")?;
        if status == "processing" {
            return Ok(PayoutLockResult::InProgress);
        }

        Ok(PayoutLockResult::InProgress)
    }

    pub async fn mark_payout_failed(&self, swap_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            UPDATE swap_address_info
            SET status = 'failed'
            WHERE swap_id = ?
              AND status = 'processing'
            "#,
        )
        .bind(swap_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Update payout status with actual amounts
    pub async fn mark_payout_completed(
        &self,
        swap_id: &str,
        tx_hash: &str,
        actual_received: f64,
        payout_amount: f64,
        commission_taken: f64,
        network_fee_paid: f64,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            UPDATE swap_address_info 
            SET status = 'success', 
                payout_tx_hash = ?,
                payout_amount = ?,
                actual_received = ?,
                commission_taken = ?,
                network_fee_paid = ?,
                commission_rate = ?,
                broadcast_at = NOW(),
                confirmed_at = NOW()
            WHERE swap_id = ?
            "#,
        )
        .bind(tx_hash)
        .bind(payout_amount)
        .bind(actual_received)
        .bind(commission_taken)
        .bind(network_fee_paid)
        .bind(if actual_received > 0.0 {
            commission_taken / actual_received
        } else {
            0.0
        })
        .bind(swap_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE swaps
            SET actual_receive = ?,
                tx_hash_out = ?,
                network_fee = ?,
                platform_fee = ?,
                total_fee = COALESCE(provider_fee, 0) + ? + ?,
                updated_at = NOW()
            WHERE id = ?
            "#,
        )
        .bind(payout_amount)
        .bind(tx_hash)
        .bind(network_fee_paid)
        .bind(commission_taken)
        .bind(commission_taken)
        .bind(network_fee_paid)
        .bind(swap_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    fn row_to_payout_asset_metadata(row: &sqlx::mysql::MySqlRow) -> PayoutAssetMetadata {
        let decimals = row.try_get::<i32, _>("decimals").unwrap_or(18);

        PayoutAssetMetadata {
            symbol: row.try_get("symbol").unwrap_or_default(),
            network: row.try_get("network").unwrap_or_default(),
            contract_address: row
                .try_get::<Option<String>, _>("contract_address")
                .unwrap_or(None),
            decimals: u8::try_from(decimals).unwrap_or(18),
            gas_multiplier: row.try_get("gas_multiplier").unwrap_or(3.0),
        }
    }

    fn row_to_payout_asset_metadata_from_currency(
        row: &sqlx::mysql::MySqlRow,
    ) -> PayoutAssetMetadata {
        let decimals = row.try_get::<i32, _>("decimals").unwrap_or(18);

        PayoutAssetMetadata {
            symbol: row.try_get("symbol").unwrap_or_default(),
            network: row.try_get("network").unwrap_or_default(),
            contract_address: row
                .try_get::<Option<String>, _>("contract_address")
                .unwrap_or(None),
            decimals: u8::try_from(decimals).unwrap_or(18),
            gas_multiplier: 3.0,
        }
    }
}
