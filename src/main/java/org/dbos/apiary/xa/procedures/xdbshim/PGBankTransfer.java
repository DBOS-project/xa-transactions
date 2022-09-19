package org.dbos.apiary.xa.procedures.xdbshim;

import java.sql.ResultSet;

import org.dbos.apiary.xa.XAConfig;
import org.dbos.apiary.xa.XAConnection;
import org.dbos.apiary.xa.XAContext;
import org.dbos.apiary.xa.XAFunction;

public class PGBankTransfer extends XAFunction {
    // Transfer one dollar from one account in one database to another account in the other database
    private final static String queryBalance = "SELECT balance FROM BankAccount WHERE id=?";
    private final static String decrementQuery = "UPDATE BankAccount SET balance = balance - 1 WHERE id=?";
    private final static String incrementQuery = "UPDATE BankAccount SET balance = balance + 1 WHERE id=?";

    public static int runFunction(org.dbos.apiary.postgres.PostgresContext context, String fromDBType, String toDBType, int fromAccountId, int toAccountId) throws Exception {
        int fromAccountBalance = 0;
        if (fromDBType == XAConnection.MySQLDBType) {
            fromAccountBalance = context.apiaryCallFunction("MySQLQueryBalance", fromAccountId).getInt();
        } else {
            ResultSet rs = context.executeQuery(fromDBType, queryBalance, fromAccountId);
            if (rs.next()) {
                fromAccountBalance = rs.getInt(1);
            }
        }

        if (fromAccountBalance <= 0) {
            return 0;
        }

        if (fromDBType == XAConnection.MySQLDBType) {
            context.apiaryCallFunction("MySQLBankTransfer", true /* decrement */, fromAccountId);
            context.executeUpdate(incrementQuery, fromAccountId);
        } else {
            context.executeUpdate(decrementQuery, fromAccountId);
            context.apiaryCallFunction("MySQLBankTransfer", false /* decrement */, fromAccountId);
        }
        return 1;
    }
}
