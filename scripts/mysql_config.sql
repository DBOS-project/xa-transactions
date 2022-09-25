-- MySQL database name and table name are case sensitive in Unix/Linux!
-- But column names are not.
SET GLOBAL innodb_lock_wait_timeout=1;
SET GLOBAL sync_binlog=1000;
SET GLOBAL innodb_flush_log_at_trx_commit=1;
SET GLOBAL innodb_buffer_pool_size=2073741824;