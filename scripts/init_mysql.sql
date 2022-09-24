-- MySQL database name and table name are case sensitive in Unix/Linux!
-- But column names are not.
CREATE DATABASE IF NOT EXISTS dbos;
USE dbos; 
SET GLOBAL innodb_lock_wait_timeout=1;
-- SET GLOBAL innodb_flush_log_at_trx_commit=2;
SET GLOBAL innodb_buffer_pool_size=1073741824;