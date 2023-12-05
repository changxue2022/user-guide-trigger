package org.stateful.test.http;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统计超过某个值的次数.不能监控多列，只能一列
 */
public class StatefulTriggerTest implements Trigger {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatefulTriggerTest.class);

    private static final String TARGET_DEVICE = "root.target.alerting.stateful";
    private Session session;
    private AtomicLong timestamp = new AtomicLong(new Date().getTime());

    // key value
    private final AtomicInteger resultCount = new AtomicInteger(0);
    private final AtomicInteger indexCount = new AtomicInteger(0);
    private static final int windownSize = 5;

    private String standardValue;
    private String tsType;
    private String trigName;
    private String iotdbHost;
    private String user = "root";
    private String password = "root";
    private List<String> measuraments = new ArrayList<>(4);
    private List<TSDataType> tsDataTypes = new ArrayList<>(4);

    private void ensureSession() throws IoTDBConnectionException {
        if (session == null ) {
            session = new Session.Builder()
                    .host(iotdbHost)
                    .username(user)
                    .password(password)
                    .build();
            session.open(false);
        }
    }

    @Override
    public void onCreate(TriggerAttributes attributes) throws Exception {
        this.tsType = attributes.getString("data_type");
        this.standardValue = attributes.getString("standard_value");
        this.trigName = attributes.getString("trig_name");
        if (attributes.hasAttribute("HOST")) {
            this.iotdbHost = attributes.getString("HOST");
        } else {
            throw new RuntimeException("HOST is required");
        }
        if (attributes.hasAttribute("user")) {
            this.user = attributes.getString("user");
        }
        if (attributes.hasAttribute("password")) {
            this.password = attributes.getString("password");
        }
        LOGGER.info("############# "+this.trigName +" CREATE ############");
        this.measuraments.add("trig_name");
        this.measuraments.add("device");
        this.measuraments.add("ts_type");
        this.measuraments.add("value");
        this.tsDataTypes.add(TSDataType.TEXT);
        this.tsDataTypes.add(TSDataType.TEXT);
        this.tsDataTypes.add(TSDataType.TEXT);
        this.tsDataTypes.add(TSDataType.TEXT);
    }

    @Override
    public void onDrop() throws Exception {
        LOGGER.info("############# "+this.trigName +" Drop ############");
        if (session != null) {
            session.close();
        }
    }

    @Override
    public boolean fire(Tablet tablet) throws Exception {
        ensureSession();
        LOGGER.info("############# "+this.trigName +" fire ############");
        List<MeasurementSchema> measurementSchemaList = tablet.getSchemas();
        List<Object> content = new ArrayList<>(2);
        for (int i = 0, n = measurementSchemaList.size(); i < n; i++) {
            switch (tsType) {
                case "double":
                    if (measurementSchemaList.get(i).getType().equals(TSDataType.DOUBLE)) {
                        // for example, we only deal with the columns of Double type
                        double[] values = (double[]) tablet.values[i];
                        for (double value : values) {
                            if (indexCount.incrementAndGet() % windownSize == 0) {
                                content.add(trigName);
                                content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                                content.add(tsType);
                                content.add(resultCount.toString());
                                this.session.insertAlignedRecord(TARGET_DEVICE, timestamp.addAndGet(2), measuraments, tsDataTypes, content);
                                content.clear();
                                resultCount.set(0);
                            }
                            if (value > Double.parseDouble(this.standardValue)) {
                                resultCount.incrementAndGet();
                            }
                        }
                    }
                    break;
                case "int32":
                    if (measurementSchemaList.get(i).getType().equals(TSDataType.INT32)) {
                        // for example, we only deal with the columns of Double type
                        int[] values = (int[]) tablet.values[i];
                        for (int value : values) {
                            if (indexCount.incrementAndGet() % windownSize == 0) {
                                content.add(trigName);
                                content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                                content.add(tsType);
                                content.add(resultCount.toString());
                                this.session.insertAlignedRecord(TARGET_DEVICE, timestamp.addAndGet(2), measuraments, tsDataTypes, content);
                                content.clear();
                                resultCount.set(0);
                            }
                            if (value > Integer.parseInt(standardValue) ) {
                                resultCount.incrementAndGet();
                            }
                        }
                    }
                    break;
                case "int64":
                    if (measurementSchemaList.get(i).getType().equals(TSDataType.INT64)) {
                        // for example, we only deal with the columns of Double type
                        long[] values = (long[]) tablet.values[i];
                        for (long value : values) {
                            if (indexCount.incrementAndGet() % windownSize == 0) {
                                content.add(trigName);
                                content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                                content.add(tsType);
                                content.add(resultCount.toString());
                                this.session.insertAlignedRecord(TARGET_DEVICE, timestamp.addAndGet(2), measuraments, tsDataTypes, content);
                                content.clear();
                                resultCount.set(0);
                            }
                            if (value > Long.parseLong(standardValue) ) {
                                resultCount.incrementAndGet();
                            }
                        }
                    }
                    break;
                case "float":
                    if (measurementSchemaList.get(i).getType().equals(TSDataType.FLOAT)) {
                        // for example, we only deal with the columns of Double type
                        float[] values = (float[]) tablet.values[i];
                        for (float value : values) {
                            if (indexCount.incrementAndGet() % windownSize == 0) {
                                content.add(trigName);
                                content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                                content.add(tsType);
                                content.add(resultCount.toString());
                                this.session.insertAlignedRecord(TARGET_DEVICE, timestamp.addAndGet(2), measuraments, tsDataTypes, content);
                                content.clear();
                                resultCount.set(0);
                            }
                            if (value > Float.parseFloat(standardValue) ) {
                                resultCount.incrementAndGet();
                            }
                        }
                    }
                    break;
                case "boolean":
                    if (measurementSchemaList.get(i).getType().equals(TSDataType.BOOLEAN)) {
                        // for example, we only deal with the columns of Double type
                        boolean[] values = (boolean[]) tablet.values[i];
                        for (boolean value : values) {
                            if (indexCount.incrementAndGet() % windownSize == 0) {
                                content.add(trigName);
                                content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                                content.add(tsType);
                                content.add(resultCount.toString());
                                this.session.insertAlignedRecord(TARGET_DEVICE, timestamp.addAndGet(2), measuraments, tsDataTypes, content);
                                content.clear();
                                resultCount.set(0);
                            }
                            if (value == Boolean.parseBoolean(standardValue) ) {
                                resultCount.incrementAndGet();
                            }
                        }
                    }
                    break;
                case "text":
                    if (measurementSchemaList.get(i).getType().equals(TSDataType.TEXT)) {
                        // for example, we only deal with the columns of Double type
                        Binary[] values = (Binary[]) tablet.values[i];
                        for (Binary value : values) {
                            if (indexCount.incrementAndGet() % windownSize == 0) {
                                content.add(trigName);
                                content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                                content.add(tsType);
                                content.add(resultCount.toString());
                                this.session.insertAlignedRecord(TARGET_DEVICE, timestamp.addAndGet(2), measuraments, tsDataTypes, content);
                                content.clear();
                                resultCount.set(0);
                            }
                            if (value.toString().equals(standardValue) ) {
                                resultCount.incrementAndGet();
                            }
                        }
                    }
                    break;
                default:
                    LOGGER.error("Invalid datatype.");
            }
        }

        return true;
    }

    @Override
    public void restore() throws Exception {
        LOGGER.info("#######" + trigName + "restore #########");
    }
}
