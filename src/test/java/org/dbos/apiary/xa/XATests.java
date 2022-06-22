package org.dbos.apiary.xa;

import com.google.protobuf.InvalidProtocolBufferException;
import org.dbos.apiary.client.ApiaryWorkerClient;
import org.dbos.apiary.utilities.ApiaryConfig;
import org.dbos.apiary.worker.ApiaryNaiveScheduler;
import org.dbos.apiary.worker.ApiaryWorker;
import org.dbos.apiary.xa.procedures.GetApiaryClientID;
import org.dbos.apiary.xa.procedures.XASimpleTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class XATests {
    private static final Logger logger = LoggerFactory.getLogger(XATests.class);

    private ApiaryWorker apiaryWorker;

    @BeforeEach
    public void resetTables() {

    }

    @AfterEach
    public void cleanupWorker() {
        if (apiaryWorker != null) {
            apiaryWorker.shutdown();
        }
    }

    @Test
    public void testSimpleXA() throws InvalidProtocolBufferException {
        logger.info("testSimpleXA");

        XAConnection conn;
        try {
            // TODO: use real connection parameters here.
            conn = new XAConnection("localhost", XAConfig.XAport, "postgres", "postgres", "dbos");
        } catch (Exception e) {
            logger.info("No XA instance!");
            return;
        }

        apiaryWorker = new ApiaryWorker(new ApiaryNaiveScheduler(), 4);
        apiaryWorker.registerConnection(XAConfig.XA, conn);
        apiaryWorker.registerFunction(ApiaryConfig.getApiaryClientID, XAConfig.XA, GetApiaryClientID::new);
        apiaryWorker.registerFunction("XASimpleTest", XAConfig.XA, XASimpleTest::new);
        apiaryWorker.startServing();

        ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");

        int res;
        res = client.executeFunction("XASimpleTest", "123").getInt();
        assertEquals(123, res);

        res = client.executeFunction("XASimpleTest", "456").getInt();
        assertEquals(456, res);
    }

}
