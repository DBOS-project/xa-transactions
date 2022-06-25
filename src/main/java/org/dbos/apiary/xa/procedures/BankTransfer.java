package org.dbos.apiary.xa.procedures;

import java.sql.ResultSet;

import org.dbos.apiary.xa.XAConnection;
import org.dbos.apiary.xa.XAContext;
import org.dbos.apiary.xa.XAFunction;

public class BankTransfer extends XAFunction {
    // Transfer one dollar from one account in one database to another account in the other database
    private final static String queryBalance = "SELECT balance FROM BankAccount WHERE id=?";
    private final static String decrementQuery = "UPDATE BankAccount SET balance = balance - 1 WHERE id=?";
    private final static String incrementQuery = "UPDATE BankAccount SET balance = balance + 1 WHERE id=?";

    public static int runFunction(XAContext context, String fromDBType, String toDBType, int fromAccountId, int toAccountId) throws Exception {
        ResultSet rs = context.executeQuery(fromDBType, queryBalance, fromAccountId);
        int fromAccountBalance = 0;
        if (rs.next()) {
            fromAccountBalance = rs.getInt(1);
        } else {
            return 0;
        }
        if (fromAccountBalance <= 0) {
            return 0;
        }

        context.executeUpdate(fromDBType, decrementQuery, fromAccountId);
        context.executeUpdate(toDBType, incrementQuery, toAccountId);
        return 1;
    }
}
