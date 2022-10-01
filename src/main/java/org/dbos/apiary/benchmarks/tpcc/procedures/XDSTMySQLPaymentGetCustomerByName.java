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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.log4j.Logger;

import org.dbos.apiary.benchmarks.tpcc.*;
import org.dbos.apiary.benchmarks.tpcc.pojo.Customer;
import org.dbos.apiary.xa.XAFunction;

import com.google.gson.Gson;

public class XDSTMySQLPaymentGetCustomerByName extends XAFunction {
    private static final Logger LOG = Logger.getLogger(XDSTMySQLPaymentGetCustomerByName.class);

    public static String customerByNameSQL =
            "SELECT *" +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_LAST = ? ";

    public static String runFunction(org.dbos.apiary.mysql.MysqlContext context, int c_w_id, int c_d_id, String customerLastName) throws Exception {
        return getCustomerByName(context, c_w_id, c_d_id, customerLastName);
    }


    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public static String getCustomerByName(org.dbos.apiary.mysql.MysqlContext context, int c_w_id, int c_d_id, String customerLastName) throws Exception {
        String customerWarehouseDBType = TPCCLoader.getDBType(c_w_id);
        assert(customerWarehouseDBType.equals(TPCCConstants.DBTYPE_MYSQL));
        ArrayList<Customer> customers = new ArrayList<Customer>();

        // customerByName.setInt(1, c_w_id);
        // customerByName.setInt(2, c_d_id);
        // customerByName.setString(3, customerLastName);
        // ResultSet rs = customerByName.executeQuery();
        ResultSet rs = context.executeQuery(customerByNameSQL, c_w_id, c_d_id, customerLastName);
        if (LOG.isTraceEnabled()) LOG.trace("C_LAST=" + customerLastName + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id);

        while (rs.next()) {
            Customer c = TPCCUtil.newCustomerFromResults(rs);
            c.c_id = rs.getInt("C_ID");
            c.c_last = customerLastName;
            customers.add(c);
        }
        rs.close();

        Collections.sort(customers, (c1, c2) -> c1.c_first.compareTo(c2.c_first));
        if (customers.size() == 0) {
            throw new RuntimeException("C_LAST=" + customerLastName + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that
        // counts starting from 1.
        int index = customers.size() / 2;
        if (customers.size() % 2 == 0) {
            index -= 1;
        }
        Gson gson = new Gson();
        return gson.toJson(customers.get(index));
    }
}