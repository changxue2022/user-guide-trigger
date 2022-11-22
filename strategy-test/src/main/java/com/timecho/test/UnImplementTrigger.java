package com.timecho.test;

import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnImplementTrigger {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnImplementTrigger.class);

    public boolean fire(Tablet tablet) throws Exception {
        return true;
    }

    public void onCreate(TriggerAttributes attributes) throws Exception {
        LOGGER.info("###### onCreate #####");
    }

}
