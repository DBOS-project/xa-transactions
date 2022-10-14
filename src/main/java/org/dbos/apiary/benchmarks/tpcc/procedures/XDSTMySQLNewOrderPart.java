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

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import org.dbos.apiary.benchmarks.tpcc.*;
import org.dbos.apiary.xa.XAContext;
import org.dbos.apiary.xa.XAFunction;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

public class XDSTMySQLNewOrderPart extends XAFunction {
    private static final Logger LOG = Logger.getLogger(XDSTMySQLNewOrderPart.class);
    
    public static final String  stmtGetItemSQL = 
        "SELECT I_PRICE, I_NAME , I_DATA " +
        "  FROM " + TPCCConstants.TABLENAME_ITEM + 
        " WHERE __apiaryid__ = ?";

    public static final String  stmtGetStockSQL = 
        "SELECT * FROM " + TPCCConstants.TABLENAME_STOCK + 
        " WHERE __apiaryid__ = ? and S_W_ID = ?";

    public static final String  stmtUpdateStockSQL = 
        "UPDATE " + TPCCConstants.TABLENAME_STOCK + 
        "   SET S_QUANTITY = ? , " +
        "       S_YTD = S_YTD + ?, " + 
        "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
        "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
        " WHERE __apiaryid__ = ? and S_W_ID = ? ";

    public String runFunction(org.dbos.apiary.mysql.MysqlContext context, int ol_supply_w_id, int ol_i_id, int ol_quantity, int ol_number, int o_ol_cnt, int w_id, int d_id) throws Exception {
		Map<String, Object> resMap = new HashMap<>();
		String ol_dist_info = new String();
		// stmtGetItem.setInt(1, ol_i_id);
		// rs = stmtGetItem.executeQuery();
		ResultSet rs = context.executeQuery(stmtGetItemSQL, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_ITEM, ol_i_id));
		if (!rs.next()) {
			// This is (hopefully) an expected error: this is an
			// expected new order rollback
			assert ol_number == o_ol_cnt;
			assert ol_i_id == TPCCConfig.INVALID_ITEM_ID;
			rs.close();
			throw new UserAbortException(
					"EXPECTED new order rollback: I_ID=" + ol_i_id
							+ " not found!");
		}

		double i_price = rs.getFloat("I_PRICE");
		String i_name = rs.getString("I_NAME");
		String i_data = rs.getString("I_DATA");
		rs.close();
		rs = null;

		// itemPrices[ol_number - 1] = i_price;
		// itemNames[ol_number - 1] = i_name;
		resMap.put("i_price", Double.valueOf(i_price));
		resMap.put("i_name", i_name);
		resMap.put("i_data", i_data);

		// stmtGetStock.setInt(1, ol_i_id);
		// stmtGetStock.setInt(2, ol_supply_w_id);
		// rs = stmtGetStock.executeQuery();
		rs = context.executeQuery(stmtGetStockSQL, TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_STOCK, ol_supply_w_id, ol_i_id), ol_supply_w_id);
		if (!rs.next())
			throw new RuntimeException("ID=" + TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_STOCK, ol_supply_w_id, ol_i_id)
					+ " not found!");
		
		int s_w_id = rs.getInt("S_W_ID");
		int s_i_id = rs.getInt("S_I_ID");
		float s_ytd = rs.getFloat("S_YTD");
		int s_order_cnt = rs.getInt("S_ORDER_CNT");
		int s_remote_cnt = rs.getInt("S_REMOTE_CNT");
		int s_quantity = rs.getInt("S_QUANTITY");
		String s_data = rs.getString("S_DATA");
		String s_dist_01 = rs.getString("S_DIST_01");
		String s_dist_02 = rs.getString("S_DIST_02");
		String s_dist_03 = rs.getString("S_DIST_03");
		String s_dist_04 = rs.getString("S_DIST_04");
		String s_dist_05 = rs.getString("S_DIST_05");
		String s_dist_06 = rs.getString("S_DIST_06");
		String s_dist_07 = rs.getString("S_DIST_07");
		String s_dist_08 = rs.getString("S_DIST_08");
		String s_dist_09 = rs.getString("S_DIST_09");
		String s_dist_10 = rs.getString("S_DIST_10");
		rs.close();
		rs = null;

		//stockQuantities[ol_number - 1] = s_quantity;
		resMap.put("old_s_quantity", s_quantity);

		if (s_quantity - ol_quantity >= 10) {
			s_quantity -= ol_quantity;
		} else {
			s_quantity += -ol_quantity + 91;
		}

		resMap.put("new_s_quantity", s_quantity);
		
		int s_remote_cnt_increment = 0;
		if (ol_supply_w_id == w_id) {
			s_remote_cnt_increment = 0;
		} else {
			s_remote_cnt_increment = 1;
		}

		s_remote_cnt += s_remote_cnt_increment;

		s_ytd += ol_quantity;

		s_order_cnt += 1;

		// stmtUpdateStock.setInt(1, s_quantity);
		// stmtUpdateStock.setInt(2, ol_quantity);
		// stmtUpdateStock.setInt(3, s_remote_cnt_increment);
		// stmtUpdateStock.setInt(4, ol_i_id);
		// stmtUpdateStock.setInt(5, ol_supply_w_id);
		// stmtUpdateStock.addBatch();
		String id = TPCCUtil.makeApiaryId(TPCCConstants.TABLENAME_STOCK, s_w_id, s_i_id);
		context.executeUpsertWithPredicate(TPCCConstants.TABLENAME_STOCK, id, "S_W_ID="+s_w_id, s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, 
				s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10);

		double ol_amount = ol_quantity * i_price;
		resMap.put("ol_amount", Double.valueOf(ol_amount));
		// orderLineAmounts[ol_number - 1] = ol_amount;
		// total_amount += ol_amount;

		if (i_data.indexOf("ORIGINAL") != -1
				&& s_data.indexOf("ORIGINAL") != -1) {
			//brandGeneric[ol_number - 1] = 'B';
			resMap.put("brandGeneric", "B");
		} else {
			//brandGeneric[ol_number - 1] = 'G';
			resMap.put("brandGeneric", "G");
		}

		

		switch ((int) d_id) {
		case 1:
			ol_dist_info = s_dist_01;
			break;
		case 2:
			ol_dist_info = s_dist_02;
			break;
		case 3:
			ol_dist_info = s_dist_03;
			break;
		case 4:
			ol_dist_info = s_dist_04;
			break;
		case 5:
			ol_dist_info = s_dist_05;
			break;
		case 6:
			ol_dist_info = s_dist_06;
			break;
		case 7:
			ol_dist_info = s_dist_07;
			break;
		case 8:
			ol_dist_info = s_dist_08;
			break;
		case 9:
			ol_dist_info = s_dist_09;
			break;
		case 10:
			ol_dist_info = s_dist_10;
			break;
		}

		resMap.put("ol_dist_info", ol_dist_info);

		Gson gson = new Gson();
		Type resMapType = new TypeToken<Map<String, Object>>() {}.getType();
		return gson.toJson(resMap, resMapType);
	}

}