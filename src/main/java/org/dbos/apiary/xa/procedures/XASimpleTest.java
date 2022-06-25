package org.dbos.apiary.xa.procedures;

import org.dbos.apiary.xa.XAConnection;
import org.dbos.apiary.xa.XAContext;
import org.dbos.apiary.xa.XAFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;


public class XASimpleTest extends XAFunction {
    private static final Logger logger = LoggerFactory.getLogger(XASimpleTest.class);

    public int runFunction(XAContext context, String input) throws Exception {
        logger.info("Simple Test, input {}", input);
        // TODO: implement meaningful logic here.
        ResultSet res1 = context.executeQuery(XAConnection.MySQLDBType, "select * from test");
        ResultSet res2 = context.executeQuery(XAConnection.PostgresDBType, "select * from test");
        // E.g., context.executeUpdates(), context.executeQuery().
        int res = Integer.parseInt(input);
        return res;
    }
}
