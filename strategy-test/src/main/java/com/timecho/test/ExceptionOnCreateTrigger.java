package com.timecho.test;

import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExceptionOnCreateTrigger implements Trigger {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionOnCreateTrigger.class);

    @Override
    public boolean fire(Tablet tablet) throws Exception {
        return true;
    }

    @Override
    public void onCreate(TriggerAttributes attributes) throws Exception {
        throw new Exception("onCreate throwing");
    }

}
