package org.dbos.apiary.xa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public abstract class BaseXAConnection implements XADBConnection {
    private static final Logger logger = LoggerFactory.getLogger(MySQLXAConnection.class);

    public abstract javax.transaction.xa.XAResource getXAResource() throws SQLException;
    public abstract java.sql.Connection getNewConnection() throws SQLException;
    public abstract java.sql.Connection getConnection() throws SQLException;
    public abstract void addUpdateTime(long time);
    public abstract void addQueryTime(long time);
    public void close() {

    }

    public void dropTable(String tableName) throws SQLException {
        Connection conn = getNewConnection();
        Statement truncateTable = conn.createStatement();
        truncateTable.execute(String.format("DROP TABLE IF EXISTS %s;", tableName));
        truncateTable.close();
        conn.close();
    }

    public void createTable(String tableName, String specStr) throws SQLException {
        Connection c = null;
        Statement s = null;
        try {
            c = getNewConnection();
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
            c = getNewConnection();
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
            getXAResource().start(xid, javax.transaction.xa.XAResource.TMNOFLAGS);
        } catch(Exception exception) {
            logger.info(exception.getMessage());
            throw exception;
        }
    }

    public void XAEnd(ApiaryXID xid) throws Exception {
        try {
            getXAResource().end(xid, javax.transaction.xa.XAResource.TMSUCCESS);
        } catch(Exception exception) {
            logger.info(exception.getMessage());
            throw exception;
        }
    }

    public void XARollback(ApiaryXID xid) throws Exception {
        try {
            getXAResource().rollback(xid);
        } catch(Exception exception) {
            logger.info(exception.getMessage());
            throw exception;
        }
    }

    public boolean XAPrepare(ApiaryXID xid) throws Exception {
        try {
            int res = getXAResource().prepare(xid);
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
            getXAResource().commit(xid, false);
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
            } else if (o instanceof Timestamp) {
                ps.setTimestamp(i + 1, (Timestamp) o);
            } else if (o instanceof Double) {
                ps.setDouble(i + 1, (Double) o);
            } else if (o instanceof Float) {
                ps.setFloat(i + 1, (Float) o);
            } else if (o instanceof Long) {
                ps.setLong(i + 1, (Long) o);
            }  else {
                assert (false); // TODO: More types.
            }
        }
    }

    /**
     * Execute a database update.
     * @param procedure a SQL DML statement (e.g., INSERT, UPDATE, DELETE).
     * @param input     input parameters for the SQL statement.
     */
    public int executeUpdate(String procedure, Object... input) throws SQLException {
        // First, prepare statement. Then, execute.
        long t0 = System.nanoTime();
        Connection c = getConnection();
        PreparedStatement pstmt = c.prepareStatement(procedure);
        prepareStatement(pstmt, input);
        int res = pstmt.executeUpdate();
        long time = System.nanoTime() - t0;
        addUpdateTime(time / 1000);
        return res;
    }

    /**
     * Execute a database query.
     * @param procedure a SQL query.
     * @param input     input parameters for the SQL statement.
     */
    public ResultSet executeQuery(String procedure, Object... input) throws SQLException {
        long t0 = System.nanoTime();
        Connection c = getConnection();
        PreparedStatement pstmt = c.prepareStatement(procedure, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        prepareStatement(pstmt, input);
        ResultSet rs = pstmt.executeQuery();
        long time = System.nanoTime() - t0;
        addQueryTime(time / 1000);
        return rs;
    }
}