package org.dbos.apiary.xa.procedures.xdbshim;

import java.sql.ResultSet;

import org.dbos.apiary.xa.XAConfig;
import org.dbos.apiary.xa.XAConnection;
import org.dbos.apiary.xa.XAContext;
import org.dbos.apiary.xa.XAFunction;

public class MySQLQueryBalance extends XAFunction {
    // Transfer one dollar from one account in one database to another account in the other database
    private final static String queryBalance = "SELECT balance FROM BankAccount WHERE id=?";
    private final static String decrementQuery = "UPDATE BankAccount SET balance = balance - 1 WHERE id=?";
    private final static String incrementQuery = "UPDATE BankAccount SET balance = balance + 1 WHERE id=?";

    public static int runFunction(org.dbos.apiary.mysql.MysqlContext context, int accountId) throws Exception {
        ResultSet res = context.executeQuery(queryBalance, accountId);
        if (res.next()) {
            return res.getInt(1);
        }
        return 0;
    }
}
