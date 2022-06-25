package org.dbos.apiary.xa;
import javax.transaction.xa.Xid;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class ApiaryXID implements Xid {
    @Override
    public String toString() {
        return String.valueOf(globalTid);
    }
    
    @Override
    public byte[] getBranchQualifier() {
        return Ints.toByteArray(0);
    }

    @Override
    public int getFormatId() {
        return 0;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return Longs.toByteArray(globalTid);
    }

    private long globalTid;

    public static ApiaryXID fromLong(long id) {
        ApiaryXID xid = new ApiaryXID();
        xid.globalTid = id;
        return xid;
    }
}
