package org.dbos.apiary.xa.procedures;

import java.sql.ResultSet;

import org.dbos.apiary.xa.XAConnection;
import org.dbos.apiary.xa.XAContext;
import org.dbos.apiary.xa.XAFunction;

public class XAQueryPersonBoth extends XAFunction {
    private final static String find = "SELECT COUNT(*) FROM PersonTable WHERE Name=? ;";

    public static int runFunction(XAContext context, String name) throws Exception {
        ResultSet rs = context.executeQuery(XAConnection.MySQLDBType, find, name);
        int count = 0;
        if (rs.next()) {
            count = rs.getInt(1);
        }

        rs = context.executeQuery(XAConnection.PostgresDBType, find, name);
        int count2 = 0;
        if (rs.next()) {
            count2 = rs.getInt(1);
        }

        return count + count2;
    }
}
