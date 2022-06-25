package org.dbos.apiary.xa;

import java.sql.SQLException;
import java.sql.ResultSet;

public abstract interface XADBConnection {
    public abstract void dropTable(String tableName) throws SQLException;

    public abstract void createTable(String tableName, String specStr) throws SQLException;

    public abstract void createIndex(String indexString) throws SQLException;

    public abstract void XAStart(ApiaryXID xid) throws Exception;

    public abstract void XAEnd(ApiaryXID xid) throws Exception;

    public abstract void XARollback(ApiaryXID xid) throws Exception;

    public abstract boolean XAPrepare(ApiaryXID xid) throws Exception;

    public abstract void XACommit(ApiaryXID xid) throws Exception;

    public abstract void executeUpdate(String procedure, Object... input) throws SQLException;

    public abstract ResultSet executeQuery(String procedure, Object... input) throws SQLException;    
}