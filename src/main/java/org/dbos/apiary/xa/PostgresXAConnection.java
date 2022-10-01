package org.dbos.apiary.xa;

import org.dbos.apiary.utilities.Percentile;
import org.postgresql.xa.PGXADataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

import javax.transaction.xa.XAResource;

public class PostgresXAConnection extends BaseXAConnection {
    private static final Logger logger = LoggerFactory.getLogger(MySQLXAConnection.class);
    public Percentile updates = new Percentile();
    public Percentile queries = new Percentile();
    private final PGXADataSource ds;
    private final ThreadLocal<javax.sql.XAConnection> xaconnection;
    private final ThreadLocal<Connection> connection;
    private static final int kIsolationLevel = Connection.TRANSACTION_SERIALIZABLE;
    public PostgresXAConnection(String hostname, Integer port, String databaseName, String databaseUsername, String databasePassword) throws SQLException {
        this.ds = new PGXADataSource();
        // Set dataSource Properties
        ds.setServerNames(new String[] {hostname});
        ds.setPortNumbers(new int[] {port});
        ds.setDatabaseName(databaseName);
        ds.setUser(databaseUsername);
        ds.setPassword(databasePassword);

        this.xaconnection = ThreadLocal.withInitial(() -> {
            try {
                javax.sql.XAConnection conn = ds.getXAConnection();
                return conn;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });

        this.connection = ThreadLocal.withInitial(() -> {
            try {
                Connection conn = this.xaconnection.get().getConnection();
                conn.setTransactionIsolation(kIsolationLevel);
                return conn;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        });

        try {
            Connection testConn = ds.getConnection();
            testConn.close();
        } catch (SQLException e) {
            logger.info("Failed to connect to Postgres");
            throw new RuntimeException("Failed to connect to Postgres");
        }
    }

    @Override
    public XAResource getXAResource() throws SQLException {
        return this.xaconnection.get().getXAResource();
    }

    @Override
    public Connection getNewConnection() throws SQLException {
        Connection c = this.ds.getConnection();
        c.setTransactionIsolation(kIsolationLevel);
        return c;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection.get();
    }

    @Override
    public void addUpdateTime(long time) {
        // TODO Auto-generated method stub
        updates.add(time);
    }

    @Override
    public void addQueryTime(long time) {
        // TODO Auto-generated method stub
        queries.add(time);
    }
}