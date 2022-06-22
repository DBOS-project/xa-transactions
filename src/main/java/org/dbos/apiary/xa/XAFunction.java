package org.dbos.apiary.xa;

import org.dbos.apiary.function.ApiaryContext;
import org.dbos.apiary.function.ApiaryFunction;
import org.dbos.apiary.function.FunctionOutput;

public class XAFunction implements ApiaryFunction {

    @Override
    public FunctionOutput apiaryRunFunction(ApiaryContext ctxt, Object... input) throws Exception {
        // TODO: may add more pre and post processing.
        FunctionOutput fo = ApiaryFunction.super.apiaryRunFunction(ctxt, input);
        return fo;
    }

    @Override
    public void recordInvocation(ApiaryContext apiaryContext, String s) {
        // Ignore this for now.
    }
}
