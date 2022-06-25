package org.dbos.apiary.xa.procedures;

import java.sql.ResultSet;

import org.dbos.apiary.xa.XAContext;
import org.dbos.apiary.xa.XAFunction;

public class BankAudit extends XAFunction {
    // Sum up balance in all accounts of two databases.
    private final static String queryBalanceSum = "SELECT sum(balance) FROM BankAccount";
    public static int runFunction(XAContext context, String DBType1, String DBType2) throws Exception {
        ResultSet rs = context.executeQuery(DBType1, queryBalanceSum);
        int sumBalance = 0;
        if (rs.next()) {
            sumBalance = rs.getInt(1);
        }

        rs = context.executeQuery(DBType2, queryBalanceSum);
        if (rs.next()) {
            sumBalance += rs.getInt(1);
        }
        return sumBalance;
    }
}
