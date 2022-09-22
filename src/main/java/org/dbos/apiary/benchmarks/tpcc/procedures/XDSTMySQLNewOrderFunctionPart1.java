/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package org.dbos.apiary.benchmarks.tpcc.procedures;

import java.sql.ResultSet;
import java.util.Random;

import org.apache.log4j.Logger;

import org.dbos.apiary.benchmarks.tpcc.*;
import org.dbos.apiary.xa.XAFunction;

public class XDSTMySQLNewOrderFunctionPart1 extends XAFunction {
    private static final Logger LOG = Logger.getLogger(XDSTMySQLNewOrderFunctionPart1.class);
    private static Random gen = new Random();

    public static final String stmtGetCustSQL = 
        "SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
        "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
        " WHERE C_W_ID = ? " + 
        "   AND C_D_ID = ? " +
        "   AND C_ID = ?";

    public static final String stmtGetWhseSQL = 
        "SELECT W_TAX " + 
        "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE + 
        " WHERE W_ID = ?";

    public static final String stmtGetDistSQL =
        "SELECT D_NEXT_O_ID, D_TAX " +
        "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
        " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE";

    public static final String stmtInsertNewOrderSQL = 
        "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
        " (NO_O_ID, NO_D_ID, NO_W_ID) " +
        " VALUES ( ?, ?, ?)";

    public static final String  stmtUpdateDistSQL = 
        "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + 
        "   SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
        " WHERE D_W_ID = ? " +
        "   AND D_ID = ?";

    public static final String  stmtInsertOOrderSQL = 
        "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER + 
        " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" + 
        " VALUES (?, ?, ?, ?, ?, ?, ?)";

    public static final String  stmtGetItemSQL = 
        "SELECT I_PRICE, I_NAME , I_DATA " +
        "  FROM " + TPCCConstants.TABLENAME_ITEM + 
        " WHERE I_ID = ?";

    public static final String  stmtGetStockSQL = 
        "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
        "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
        "  FROM " + TPCCConstants.TABLENAME_STOCK + 
        " WHERE S_I_ID = ? " +
        "   AND S_W_ID = ? FOR UPDATE";

    public static final String  stmtUpdateStockSQL = 
        "UPDATE " + TPCCConstants.TABLENAME_STOCK + 
        "   SET S_QUANTITY = ? , " +
        "       S_YTD = S_YTD + ?, " + 
        "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
        "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
        " WHERE S_I_ID = ? " +
        "   AND S_W_ID = ?";

    public static final String  stmtInsertOrderLineSQL = 
    "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE + 
    " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
    " VALUES (?,?,?,?,?,?,?,?,?)";

    public static int runFunction(org.dbos.apiary.mysql.MysqlContext context, int terminalWarehouseID, int numWarehouses) throws Exception {
		
		return 0;
    }

}