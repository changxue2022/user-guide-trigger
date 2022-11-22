package com.timecho.test;

import com.sun.media.jfxmedia.logging.Logger;
import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.LoggerFactory;

public class FireReturnFalseOpti implements Trigger {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FireReturnFalseOpti.class);
    @Override
    public boolean fire(Tablet tablet)  {
        LOGGER.info("@@@@@@@@@@@ FireReturnFalseOpti fire @@@@@@@@@@");
        return false;
    }

    @Override
    public FailureStrategy getFailureStrategy() {
        return FailureStrategy.OPTIMISTIC;
    }
}
