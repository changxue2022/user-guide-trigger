package com.timecho.test;

import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.LoggerFactory;

public class FireReturnFalsePess implements Trigger {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FireReturnFalsePess.class);
    @Override
    public boolean fire(Tablet tablet) throws Exception {
        LOGGER.info("@@@@@@@@@@@ FireReturnFalsePess fire @@@@@@@@@@");
        return false;
    }

    @Override
    public FailureStrategy getFailureStrategy() {
        return FailureStrategy.PESSIMISTIC;
    }
}
