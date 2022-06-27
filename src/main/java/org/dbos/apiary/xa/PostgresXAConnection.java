package org.dbos.apiary.xa;

import org.postgresql.xa.PGXADataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

import javax.transaction.xa.XAResource;

public class PostgresXAConnection extends BaseXAConnection {
    private static final Logger logger = LoggerFactory.getLogger(MySQLXAConnection.class);

    private final PGXADataSource ds;
    private final ThreadLocal<javax.sql.XAConnection> xaconnection;
    private final ThreadLocal<Connection> connection;

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
        return this.ds.getConnection();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection.get();
    }
}