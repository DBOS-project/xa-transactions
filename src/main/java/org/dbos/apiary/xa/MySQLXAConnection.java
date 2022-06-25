package org.dbos.apiary.xa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.jdbc.MysqlXADataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQLXAConnection implements XADBConnection {
    private static final Logger logger = LoggerFactory.getLogger(MySQLXAConnection.class);

    private final ThreadLocal<MysqlXADataSource> ds;
    private final ThreadLocal<javax.sql.XAConnection> xaconnection;
    private final ThreadLocal<Connection> connection;

    public MySQLXAConnection(String hostname, Integer port, String databaseName, String databaseUsername, String databasePassword) throws SQLException {
        this.ds = ThreadLocal.withInitial(() -> {
            MysqlXADataSource ds = new MysqlXADataSource();
            // Set dataSource Properties
            ds.setServerName(hostname);
            ds.setPortNumber(port);
            ds.setDatabaseName(databaseName);
            ds.setUser(databaseUsername);
            ds.setPassword(databasePassword);
            return ds;
        });

        this.xaconnection = ThreadLocal.withInitial(() -> {
            try {
                javax.sql.XAConnection conn = ds.get().getXAConnection();
                // conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                return conn;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });

        this.connection = ThreadLocal.withInitial(() -> {
            try {
                Connection conn = this.xaconnection.get().getConnection();
                conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                return conn;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });

        try {
            Connection testConn = ds.get().getConnection();
            testConn.close();
        } catch (SQLException e) {
            logger.info("Failed to connect to Postgres");
            throw new RuntimeException("Failed to connect to Postgres");
        }
    }

    public void dropTable(String tableName) throws SQLException {
        Connection conn = ds.get().getConnection();
        Statement truncateTable = conn.createStatement();
        truncateTable.execute(String.format("DROP TABLE IF EXISTS %s;", tableName));
        truncateTable.close();
        conn.close();
    }

    public void createTable(String tableName, String specStr) throws SQLException {
        Connection c = null;
        Statement s = null;
        try {
            c = ds.get().getConnection();
            s = c.createStatement();
            String apiaryTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s);", tableName,specStr);
            s.execute(apiaryTable);
        } catch(Exception e) {
            logger.info(e.getMessage());
            throw e;
        } finally {
            if (s != null) {
                s.close();
            }
            if (c != null) {
                c.close();
            }
        }
    }

    public void createIndex(String indexString) throws SQLException {
        Connection c = null;
        Statement s = null;
        try {
            c = ds.get().getConnection();
            s = c.createStatement();
            s.execute(indexString);
        } catch(Exception e) {
            logger.info(e.getMessage());
            throw e;
        } finally {
            if (s != null) {
                s.close();
            }
            if (c != null) {
                c.close();
            }
        }
    }

    public void XAStart(ApiaryXID xid) throws Exception {
        try {
            this.xaconnection.get().getXAResource().start(xid, javax.transaction.xa.XAResource.TMNOFLAGS);
        } catch(Exception exception) {
            logger.info(exception.getMessage());
            throw exception;
        }
    }

    public void XAEnd(ApiaryXID xid) throws Exception {
        try {
            this.xaconnection.get().getXAResource().end(xid, javax.transaction.xa.XAResource.TMSUCCESS);
        } catch(Exception exception) {
            logger.info(exception.getMessage());
            throw exception;
        }
    }

    public void XARollback(ApiaryXID xid) throws Exception {
        try {
            this.xaconnection.get().getXAResource().rollback(xid);
        } catch(Exception exception) {
            logger.info(exception.getMessage());
            throw exception;
        }
    }

    public boolean XAPrepare(ApiaryXID xid) throws Exception {
        try {
            int res = this.xaconnection.get().getXAResource().prepare(xid);
            if (res == javax.transaction.xa.XAResource.XA_OK || res == javax.transaction.xa.XAResource.XA_RDONLY) {
                return true;
            }
        } catch(Exception exception) {
            logger.info(exception.getMessage());
            throw exception;
        }
        return false;
    }

    public void XACommit(ApiaryXID xid) throws Exception  {
        try {
            this.xaconnection.get().getXAResource().commit(xid, false);
        } catch(Exception exception) {
            logger.info(exception.getMessage());
        }
    }

    private void prepareStatement(PreparedStatement ps, Object[] input) throws SQLException {
        for (int i = 0; i < input.length; i++) {
            Object o = input[i];
            if (o instanceof Integer) {
                ps.setInt(i + 1, (Integer) o);
            } else if (o instanceof String) {
                ps.setString(i + 1, (String) o);
            } else {
                assert (false); // TODO: More types.
            }
        }
    }

    /**
     * Execute a database update.
     * @param procedure a SQL DML statement (e.g., INSERT, UPDATE, DELETE).
     * @param input     input parameters for the SQL statement.
     */
    public void executeUpdate(String procedure, Object... input) throws SQLException {
        // First, prepare statement. Then, execute.
        PreparedStatement pstmt = this.connection.get().prepareStatement(procedure);
        prepareStatement(pstmt, input);
        pstmt.executeUpdate();
    }

    /**
     * Execute a database query.
     * @param procedure a SQL query.
     * @param input     input parameters for the SQL statement.
     */
    public ResultSet executeQuery(String procedure, Object... input) throws SQLException {
        PreparedStatement pstmt = this.connection.get().prepareStatement(procedure, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        prepareStatement(pstmt, input);
        ResultSet rs = pstmt.executeQuery();
        return rs;
    }
}