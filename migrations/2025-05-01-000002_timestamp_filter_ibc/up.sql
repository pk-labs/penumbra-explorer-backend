-- Drop views in correct dependency order
DROP VIEW IF EXISTS ibc_client_stats_with_periods;
DROP VIEW IF EXISTS ibc_client_summary_30d;
DROP VIEW IF EXISTS ibc_client_summary_24h;
DROP VIEW IF EXISTS ibc_client_summary;
DROP VIEW IF EXISTS explorer_transaction_summary;
DROP VIEW IF EXISTS explorer_recent_blocks;

-- Drop tables with foreign key dependencies first
DROP TABLE IF EXISTS ibc_transfers;
DROP TABLE IF EXISTS ibc_stats;
DROP TABLE IF EXISTS ibc_channels;
DROP TABLE IF EXISTS explorer_transactions;
DROP TABLE IF EXISTS ibc_clients;
DROP TABLE IF EXISTS explorer_block_details;
DROP TABLE IF EXISTS index_watermarks;
DROP TABLE IF EXISTS cometindex_tracking;
