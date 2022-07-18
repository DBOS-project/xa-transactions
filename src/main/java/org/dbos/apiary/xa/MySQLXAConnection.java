package org.dbos.apiary.xa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.jdbc.MysqlXADataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.transaction.xa.XAResource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQLXAConnection extends BaseXAConnection  {
    private static final Logger logger = LoggerFactory.getLogger(MySQLXAConnection.class);

    private final MysqlXADataSource ds;
    private final ThreadLocal<javax.sql.XAConnection> xaconnection;
    private final ThreadLocal<Connection> connection;
    private static final int kIsolationLevel = Connection.TRANSACTION_SERIALIZABLE;
    public MySQLXAConnection(String hostname, Integer port, String databaseName, String databaseUsername, String databasePassword) throws SQLException {
        ds = new MysqlXADataSource();
        // Set dataSource Properties
        ds.setServerName(hostname);
        ds.setPortNumber(port);
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
}