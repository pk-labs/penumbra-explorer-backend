-- This is a no-op down migration as the tables are recreated by the application
-- The up migration drops tables, this down migration does nothing

-- If we need to explicitly drop the validators tables:
-- DROP VIEW IF EXISTS validator_performance;
-- DROP TABLE IF EXISTS validator_blocks;
-- DROP TABLE IF EXISTS validators;