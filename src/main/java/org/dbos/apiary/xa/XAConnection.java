package org.dbos.apiary.xa;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public class XAConnection implements ApiaryConnection {

    private static final Logger logger = LoggerFactory.getLogger(XAConnection.class);

    public XAConnection(String hostname, Integer port, String databaseName, String databaseUsername, String databasePassword) throws SQLException {
        // TODO: implement building connections to databases.
        // Please refer to the implementation of PostgresConnection.
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, String service, long execID, long functionID, Object... inputs) throws Exception {
        // TODO: may pass more parameters to XAContext.
        XAContext ctxt = new XAContext(workerContext, service, execID, functionID);
        FunctionOutput f = null;
        try {
            // The function would contain transactions across multiple databases.
            f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // TODO: transaction manager does 2PC here to commit or abort the transaction.
        return f;
    }

    @Override
    public Set<TransactionContext> getActiveTransactions() {
        // Ignore this if not necessary.
        return null;
    }

    @Override
    public void updatePartitionInfo() {
        // Ignore this if not necessary.
    }

    @Override
    public int getNumPartitions() {
        // Ignore this if not necessary.
        return 1;
    }

    @Override
    public String getHostname(Object... objects) {
        // Ignore this if not necessary.
        return "localhost";
    }

    @Override
    public Map<Integer, String> getPartitionHostMap() {
        // Ignore this if not necessary.
        return Map.of(0, "localhost");
    }
}
