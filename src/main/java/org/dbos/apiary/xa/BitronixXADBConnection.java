package org.dbos.apiary.xa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bitronix.tm.resource.jdbc.PoolingDataSource;

import java.sql.SQLException;
import java.util.Properties;

import javax.transaction.xa.XAResource;

import java.sql.Connection;

public class BitronixXADBConnection extends BaseXAConnection  {
    private static final Logger logger = LoggerFactory.getLogger(MySQLXAConnection.class);

    private PoolingDataSource ds;
    //private final ThreadLocal<javax.sql.XAConnection> xaconnection;
    private final ThreadLocal<Connection> connection;

    public void close() {
        ds.close();
    }

    public BitronixXADBConnection(String XAUniqueResourceName, String XADataSourceClassName, String hostname, Integer port, String databaseName, String databaseUsername, String databasePassword) throws SQLException {
        ds = new PoolingDataSource();
		ds.setClassName(XADataSourceClassName);
		ds.setUniqueName(XAUniqueResourceName);
		ds.setMaxPoolSize(16);
		ds.getDriverProperties().setProperty("databaseName", databaseName);
        ds.getDriverProperties().setProperty("serverName", hostname);
        ds.getDriverProperties().setProperty("portNumber", String.valueOf(port));
        ds.getDriverProperties().setProperty("user", databaseUsername);
        ds.getDriverProperties().setProperty("password", databasePassword);
        ds.setAutomaticEnlistingEnabled(true);
        ds.setAllowLocalTransactions(true);

		ds.init();

        // // Set dataSource Properties
        // ds.setUniqueResourceName(XAUniqueResourceName);
        // ds.setXaDataSourceClassName(XADataSourceClassName);
        // ds.setPoolSize(16);
        // Properties p = new Properties();
        // p.setProperty("user", databaseUsername);
        // p.setProperty("password", databasePassword);
        // p.setProperty("portNumber", String.valueOf(port));
        // p.setProperty("databaseName", databaseName);
        // p.setProperty("serverName", hostname);
        // ds.setXaProperties(p);
        // // ds.setServerName(hostname);
        // // ds.setPortNumber(port);
        // // ds.setDatabaseName(databaseName);
        // // ds.setUser(databaseUsername);
        // // ds.setPassword(databasePassword);

        // this.xaconnection = ThreadLocal.withInitial(() -> {
        //     try {
        //         javax.sql.XAConnection conn = ds.getXaDataSource().getXAConnection();
        //         // conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        //         return conn;
        //     } catch (SQLException e) {
        //         e.printStackTrace();
        //     }
        //     return null;
        // });

        this.connection = ThreadLocal.withInitial(() -> {
            try {
                Connection conn = this.ds.getConnection();
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
        return null;
    }

    @Override
    public Connection getNewConnection() throws SQLException {
        return ds.getConnection();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection.get();
    }
}