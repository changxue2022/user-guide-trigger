package com.timecho.test;

import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionTrigger implements Trigger {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionTrigger.class);
    @Override
    public void restore() throws Exception {
        LOGGER.info("########## restore  ##########");
        throw new Exception("restore throwing");
    }

    @Override
    public boolean fire(Tablet tablet) throws Exception {
        return false;
    }

    @Override
    public FailureStrategy getFailureStrategy() {
        LOGGER.info("########## getFailureStrategy  ##########");
        double t = 33.00/0;
        return FailureStrategy.PESSIMISTIC;
    }
}
