package org.dbos.apiary.xa.procedures;

import org.dbos.apiary.xa.XAContext;
import org.dbos.apiary.xa.XAFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetApiaryClientID extends XAFunction {
    private static final Logger logger = LoggerFactory.getLogger(GetApiaryClientID.class);

    public static int runFunction(XAContext context) throws Exception {
        logger.info("Get Apiary ID!");
        return 1;
    }
}
