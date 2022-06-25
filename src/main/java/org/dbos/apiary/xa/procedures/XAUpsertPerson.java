package org.dbos.apiary.xa.procedures;

import java.sql.ResultSet;

import org.dbos.apiary.xa.XAConnection;
import org.dbos.apiary.xa.XAContext;
import org.dbos.apiary.xa.XAFunction;

public class XAUpsertPerson extends XAFunction {
    private final static String insert = "INSERT INTO PersonTable VALUES(?,?);";

    public static int runFunction(XAContext context, String name, int number) throws Exception {
        context.executeUpdate(XAConnection.MySQLDBType, insert, name, number);
        context.executeUpdate(XAConnection.PostgresDBType, insert, name, number);
        return number;
    }
}
