package org.dbos.apiary.xa.procedures;

import org.dbos.apiary.xa.XAContext;
import org.dbos.apiary.xa.XAFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class XASimpleTest extends XAFunction {
    private static final Logger logger = LoggerFactory.getLogger(XASimpleTest.class);

    // TODO: add some SQL queries here.

    public int runFunction(XAContext context, String input) throws Exception {
        logger.info("Simple Test, input {}", input);
        // TODO: implement meaningful logic here.
        // E.g., context.executeUpdates(), context.executeQuery().
        int res = Integer.parseInt(input);
        return res;
    }
}
