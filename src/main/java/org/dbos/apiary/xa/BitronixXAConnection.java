package org.dbos.apiary.xa;

import org.dbos.apiary.benchmarks.tpcc.UserAbortException;

import org.dbos.apiary.connection.ApiaryConnection;
import org.dbos.apiary.function.FunctionOutput;
import org.dbos.apiary.function.TransactionContext;
import org.dbos.apiary.function.WorkerContext;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dbos.apiary.utilities.ScopedTimer;
import org.dbos.apiary.utilities.Tracer;

import com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class BitronixXAConnection extends XAConnection {

    private static final Logger logger = LoggerFactory.getLogger(XAConnection.class);
    private boolean disableBitronix = false;
    public static String PostgresDBType = "postgres";
    public static String MySQLDBType = "mysql";

    BitronixXADBConnection pgConnection;
    BitronixXADBConnection mqConnection;
    BitronixTransactionManager btm = null;

    public void disableXA() {
        this.disableBitronix = true;
    }

    @Override
    public XADBConnection getXAConnection(String DBType) {
        if (DBType.equals(PostgresDBType)) {
            return this.pgConnection;
        } else if (DBType.equals(MySQLDBType)) {
            return this.mqConnection;
        } else {
            return null;
        }
    }
    
    public BitronixXAConnection(BitronixXADBConnection postgresConnection, BitronixXADBConnection mysqlConnection) throws SQLException {
        super(null,  null);
        btm = TransactionManagerServices.getTransactionManager();
        this.pgConnection = postgresConnection;
        this.mqConnection = mysqlConnection;
        assert(this.pgConnection != null);
        assert(this.mqConnection != null);
    }

    public BitronixXAConnection(BitronixXADBConnection postgresConnection, BitronixXADBConnection mysqlConnection, boolean disableXA) throws SQLException {
        super(null,  null);
        if (disableXA) {
            disableBitronix = true;
        } else {
            btm = TransactionManagerServices.getTransactionManager();
        }
        this.pgConnection = postgresConnection;
        this.mqConnection = mysqlConnection;
        assert(this.pgConnection != null);
        assert(this.mqConnection != null);
    }

    @Override
    public FunctionOutput callFunction(String functionName, WorkerContext workerContext, String service, long execID, long functionID, boolean isReplay, Object... inputs) throws Exception {
        FunctionOutput f = null;
        long t0 = System.nanoTime();
        XAContext succeededCtx = null;
        AtomicLong totalTime = new AtomicLong(0);
        long t00 = System.nanoTime();
        while(true) {
            try (ScopedTimer t000 = new ScopedTimer((long elapsed) -> totalTime.set(elapsed))) {
                XAContext ctxt = new XAContext(this, workerContext, service, execID, functionID);
                succeededCtx = ctxt;
                try {
                    
                    if (disableBitronix == false) {
                        try (ScopedTimer t1 = new ScopedTimer((long elapsed) -> ctxt.initializationNanos.set(elapsed) )) {
                            btm.begin();
                        } catch (Exception e) {
                            throw e;
                        }
                        
                        try (ScopedTimer t2 = new ScopedTimer((long elapsed) -> ctxt.executionNanos.set(elapsed) )) {
                            f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
                        } catch (Exception e) {
                            throw e;
                        }
                        
                        try (ScopedTimer t3 = new ScopedTimer((long elapsed) -> ctxt.commitNanos.set(elapsed) )) {
                            btm.commit(ctxt.commitPrepareNanos);
                        } catch (Exception e) {
                            throw e;
                        }
                    } else {
                        f = workerContext.getFunction(functionName).apiaryRunFunction(ctxt, inputs);
                        ctxt.commitAllConnections();
                    }
                    break;
                } catch (Exception e) {
                    try {
                        if (disableBitronix == false) {
                            if (btm.getCurrentTransaction() != null) {
                                btm.rollback();
                            }
                        } else {
                            ctxt.rollbackAllConnections();
                        }
                    } catch (Exception ex) {
                        logger.info("rollback error");
                        ex.printStackTrace();
                    }
                    if (e instanceof InvocationTargetException) {
                        Throwable innerException = e;
                        while (innerException instanceof InvocationTargetException) {
                            InvocationTargetException i = (InvocationTargetException) innerException;
                            innerException = i.getCause();
                        }
                        if (innerException instanceof PSQLException) {
                            PSQLException p = (PSQLException) innerException;
                            if (p.getSQLState().equals(PSQLState.SERIALIZATION_FAILURE.getState())) {
                                continue;
                            } else {
                                logger.info("Unrecoverable XA error from PG: {} {}", p.getMessage(), p.getSQLState());
                            }
                        } else if (innerException instanceof MySQLTransactionRollbackException) {
                            MySQLTransactionRollbackException m = (MySQLTransactionRollbackException) innerException;
                            if (m.getErrorCode() == 1213 || m.getErrorCode() == 1205) {
                                continue; // Deadlock or lock timed out
                            } else {
                                logger.info("Unrecoverable XA error from MySQL: {} {} {}", m.getMessage(), m.getSQLState(), m.getErrorCode());
                            }
                        } else if (innerException instanceof UserAbortException) {
                            continue;
                        }
                    } else if (e instanceof bitronix.tm.internal.BitronixRollbackException) {
                        continue;
                    } else if (e instanceof UserAbortException) {
                        continue;
                    }
                    logger.info("Unrecoverable error in function execution: {}", e.toString());

                    e.printStackTrace();
                    break;
                }
            } catch (Exception e) {
                // TODO: handle exception
            }
            
        }
        funcCalls.add((System.nanoTime() - t0) / 1000);
        totalTime.set(System.nanoTime() - t00);
        //logger.info("functionName {}, execID {}", functionName, execID);
        if (succeededCtx != null && tracer != null) {
            tracer.setTotalTime(execID, totalTime.get());
            tracer.setXAInitNanos(execID, succeededCtx.initializationNanos.get());
            tracer.setXAExecutionNanos(execID, succeededCtx.executionNanos.get());
            tracer.setXACommitNanos(execID, succeededCtx.commitNanos.get());
            tracer.setXACommitPrepareNanos(execID, succeededCtx.commitPrepareNanos.get());
            tracer.addCategoryValidId(functionName, execID);
        }
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
        Map<Integer, String> myMap = new HashMap<Integer, String>() {{
            put(0, "localhost");
        }};
        return myMap;
    }
}
