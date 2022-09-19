package org.dbos.apiary.benchmarks.tpcc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;

public interface CrossDBTransactionManager {
    public void begin();
    public void commit();
    public void rollback();
    public PreparedStatement getPreparedStatement(String DBType, String SQL) throws SQLException;
    public Connection getRawConnection(String DBType) throws SQLException;
}
