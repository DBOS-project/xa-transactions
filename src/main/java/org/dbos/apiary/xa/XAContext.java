package org.dbos.apiary.xa;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dbos.apiary.xa.MySQLXAConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class XAContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(XAContext.class);
    private XAConnection xaConn;
    public XAContext(XAConnection xaConn, WorkerContext workerContext, String service, long execID, long functionID) {
        super(workerContext, service, execID, functionID, false);
        this.xaConn = xaConn;
    }

    public int executeUpdate(String DBType, String procedure, Object... input) throws SQLException {
        // TODO: implement executing updates here.
        return xaConn.getXAConnection(DBType).executeUpdate(procedure, input);
    }

    public void recordExecution(org.dbos.apiary.function.FunctionOutput arg0) {}

    public org.dbos.apiary.function.FunctionOutput checkPreviousExecution() {
        return null;
    }

    public ResultSet executeQuery(String DBType, String procedure, Object... input) throws SQLException {
        return xaConn.getXAConnection(DBType).executeQuery(procedure, input);
    }

    @Override
    public FunctionOutput apiaryCallFunction(String s, Object... objects) throws Exception {
        // TODO: implement later. Call another function from within a function.
        return null;
    }
}
