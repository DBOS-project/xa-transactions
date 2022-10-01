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


public class XDSTMySQLPaymentPart extends XAFunction {
    private static final Logger LOG = Logger.getLogger(XDSTMySQLPaymentPart.class);
    private static Random gen = new Random();

    public static String payUpdateWhseSQL = 
            "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE + 
            "   SET W_YTD = W_YTD + ? " +
            " WHERE ID = ? ";
    
    public static String payGetWhseSQL = 
            "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" + 
            "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE + 
            " WHERE ID = ?";
    
    public static String payUpdateDistSQL = 
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT + 
            "   SET D_YTD = D_YTD + ? " +
            " WHERE ID = ? ";
    
    public static String payGetDistSQL = 
            "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" + 
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT + 
            " WHERE ID = ? ";
    
    public static String payGetCustSQL = 
            "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " + 
            "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " + 
            "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?";
    
    public static String payGetCustCdataSQL = 
            "SELECT C_DATA " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
            " WHERE __apiaryID__ =? and C_W_ID = ?";
    
    public static String payUpdateCustBalCdataSQL = 
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + 
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " + 
            "       C_PAYMENT_CNT = ?, " +
            "       C_DATA = ? " +
            " WHERE ID = ?";
    
    public static String payUpdateCustBalSQL =
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER + 
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " +
            "       C_PAYMENT_CNT = ? " +
            " WHERE ID = ?";
    
    public static String payInsertHistSQL = 
            "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY + 
            " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
            " VALUES (?,?,?,?,?,?,?,?)";
    
    public static String customerByNameSQL =
            "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " + 
            "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
            "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_LAST = ? ";

    public static int runFunction(org.dbos.apiary.mysql.MysqlContext context, String customerJson, int customerWarehouseID, int customerDistrictID, int w_id, int districtID, float paymentAmount) throws Exception {
        Gson gson = new Gson();
        Customer c = gson.fromJson(customerJson, Customer.class);
        String c_data = null;
        if (c.c_credit.equals("BC")) { // bad credit
            // payGetCustCdata.setInt(1, customerWarehouseID);
            // payGetCustCdata.setInt(2, customerDistrictID);
            // payGetCustCdata.setInt(3, c.c_id);
            // rs = payGetCustCdata.executeQuery();
            String id = TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_CUSTOMER, customerWarehouseID, customerDistrictID, c.c_id);
            ResultSet rs = context.executeQuery(payGetCustCdataSQL, id, customerWarehouseID);
            if (!rs.next())
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");
            c_data = rs.getString("C_DATA");
            rs.close();
            rs = null;

            c_data = c.c_id + " " + customerDistrictID + " " + customerWarehouseID + " " + districtID + " " + w_id + " " + paymentAmount + " | " + c_data;
            if (c_data.length() > 500)
                c_data = c_data.substring(0, 500);

            // payUpdateCustBalCdata.setDouble(1, c.c_balance);
            // payUpdateCustBalCdata.setDouble(2, c.c_ytd_payment);
            // payUpdateCustBalCdata.setInt(3, c.c_payment_cnt);
            // payUpdateCustBalCdata.setString(4, c_data);
            // payUpdateCustBalCdata.setInt(5, customerWarehouseID);
            // payUpdateCustBalCdata.setInt(6, customerDistrictID);
            // payUpdateCustBalCdata.setInt(7, c.c_id);
            // result = payUpdateCustBalCdata.executeUpdate();
            
            //result = context.executeUpdate(customerWarehouseDBType, payUpdateCustBalCdataSQL, c.c_balance, c.c_ytd_payment, c.c_payment_cnt, c_data, customerWarehouseID, customerDistrictID, c.c_id);
            //context.executeUpdate(payUpdateCustBalCdataSQL, TPCCUtil.concatenate(c.c_w_id, c.c_d_id, c.c_id), c.c_balance, c.c_ytd_payment, c.c_payment_cnt, c_data, customerWarehouseID, customerDistrictID, c.c_id);
            c.c_data = c_data;
            id = TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_CUSTOMER, c.c_w_id, c.c_d_id, c.c_id);
            context.executeUpsert(TPCCConstants.TABLENAME_CUSTOMER, id, 
                                c.c_w_id, c.c_d_id, c.c_id, c.c_discount, 
                                c.c_credit, c.c_last, c.c_first, 
                                c.c_credit_lim, c.c_balance, c.c_ytd_payment, 
                                c.c_payment_cnt, c.c_delivery_cnt, c.c_street_1, 
                                c.c_street_2, c.c_city, c.c_state, c.c_zip, 
                                c.c_phone, c.c_since, c.c_middle, c.c_data);
            // if (result == 0)
            //     throw new RuntimeException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID);

        } else { // GoodCredit

            // payUpdateCustBal.setDouble(1, c.c_balance);
            // payUpdateCustBal.setDouble(2, c.c_ytd_payment);
            // payUpdateCustBal.setInt(3, c.c_payment_cnt);
            // payUpdateCustBal.setInt(4, customerWarehouseID);
            // payUpdateCustBal.setInt(5, customerDistrictID);
            // payUpdateCustBal.setInt(6, c.c_id);
            // result = payUpdateCustBal.executeUpdate();
            // result = context.executeUpdate(customerWarehouseDBType, payUpdateCustBalSQL, c.c_balance, c.c_ytd_payment, c.c_payment_cnt, customerWarehouseID, customerDistrictID, c.c_id);
            // if (result == 0)
            //     throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");
            //context.executeUpdate(payUpdateCustBalSQL, c.c_balance, c.c_ytd_payment, c.c_payment_cnt, customerWarehouseID, customerDistrictID, c.c_id);
            String id = TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_CUSTOMER, c.c_w_id, c.c_d_id, c.c_id);
            if (c.c_data == null) {
                LOG.info("c.c_data is null");
            }
            if (c.c_middle == null) {
                LOG.info("c.c_middle is null");
            }
            if (c.c_middle == null) {
                LOG.info("c.c_middle is null");
            }
            context.executeUpsert(TPCCConstants.TABLENAME_CUSTOMER, id, 
                                c.c_w_id, c.c_d_id, c.c_id, c.c_discount, 
                                c.c_credit, c.c_last, c.c_first, 
                                c.c_credit_lim, c.c_balance, c.c_ytd_payment, 
                                c.c_payment_cnt, c.c_delivery_cnt, c.c_street_1, 
                                c.c_street_2, c.c_city, c.c_state, c.c_zip, 
                                c.c_phone, c.c_since, c.c_middle, c.c_data);

        }

        return 0;
    }
}