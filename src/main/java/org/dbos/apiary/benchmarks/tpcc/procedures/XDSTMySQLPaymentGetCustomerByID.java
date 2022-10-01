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
import org.dbos.apiary.benchmarks.tpcc.pojo.Customer;
import org.dbos.apiary.xa.XAFunction;

import com.google.gson.Gson;

public class XDSTMySQLPaymentGetCustomerByID extends XAFunction {
    private static final Logger LOG = Logger.getLogger(XDSTMySQLPaymentGetCustomerByID.class);

    public static String payGetCustSQL = 
            "SELECT * " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
            " WHERE __apiaryid__ = ? and C_W_ID = ?";
    
    public static String runFunction(org.dbos.apiary.mysql.MysqlContext context, int c_w_id, int c_d_id, int c_id) throws Exception {
        return getCustomerById(context, c_w_id, c_d_id, c_id);
    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public static String getCustomerById(org.dbos.apiary.mysql.MysqlContext context, int c_w_id, int c_d_id, int c_id) throws Exception {
        String customerWarehouseDBType = TPCCLoader.getDBType(c_w_id);
        assert(customerWarehouseDBType.equals(TPCCConstants.DBTYPE_MYSQL));
        // payGetCust.setInt(1, c_w_id);
        // payGetCust.setInt(2, c_d_id);
        // payGetCust.setInt(3, c_id);
        // ResultSet rs = payGetCust.executeQuery();
        ResultSet rs = context.executeQuery(payGetCustSQL, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_CUSTOMER, c_w_id, c_d_id, c_id), c_w_id);
        if (!rs.next()) {
            throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        Customer c = TPCCUtil.newCustomerFromResults(rs);
        c.c_id = c_id;
        c.c_last = rs.getString("C_LAST");
        rs.close();
        Gson gson = new Gson();
        return gson.toJson(c);
    }

}