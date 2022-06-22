package org.dbos.apiary.xa;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class XAContext extends ApiaryContext {
    private static final Logger logger = LoggerFactory.getLogger(XAContext.class);

    public XAContext(WorkerContext workerContext, String service, long execID, long functionID) {
        super(workerContext, service, execID, functionID);
    }

    public void executeUpdate(String procedure, Object... input) throws SQLException {
        // TODO: implement executing updates here.
    }

    public ResultSet executeQuery(String procedure, Object... input) throws SQLException {
        // TODO: implement executing queries here.
        return null;
    }

    @Override
    public FunctionOutput apiaryCallFunction(String s, Object... objects) throws Exception {
        // TODO: implement later. Call another function from within a function.
        return null;
    }
}
