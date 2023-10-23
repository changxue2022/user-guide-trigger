package org.apache.iotdb.db.protocol.mqtt;

import java.util.List;

public class PayloadJson {
    private String device;
    private Long timestamp;
    private List<String> measurements;
    private List<String> values;

    public String getDevice() {
        return device;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public List<String> getMeasurements() {
        return measurements;
    }

    public List<String> getValues() {
        return values;
    }
}
