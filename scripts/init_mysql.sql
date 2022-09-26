-- MySQL database name and table name are case sensitive in Unix/Linux!
-- But column names are not.
CREATE DATABASE IF NOT EXISTS dbos;
USE dbos; 
SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SET GLOBAL innodb_lock_wait_timeout=1; 