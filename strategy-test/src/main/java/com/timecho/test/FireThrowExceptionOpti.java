package com.timecho.test;


import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.LoggerFactory;

public class FireThrowExceptionOpti implements Trigger {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FireThrowExceptionOpti.class);
    @Override
    public boolean fire(Tablet tablet) throws Exception {
        LOGGER.info("@@@@@@@@@@@ FireThrowExceptionOpti fire @@@@@@@@@@");
        throw new Exception("expect this trigger stopping this time");
    }

    @Override
    public FailureStrategy getFailureStrategy() {
        return FailureStrategy.OPTIMISTIC;
    }
}
