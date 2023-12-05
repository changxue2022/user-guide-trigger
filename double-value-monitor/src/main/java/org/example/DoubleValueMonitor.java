package org.example;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 监控某列值，大于hi 的报Critical, 大于lo小于 hi 的报 Warn
 * 写入root.ln.alerting device中
 * 支持一个device的多列
 */
public class DoubleValueMonitor implements Trigger {
    private static final Logger LOGGER = LoggerFactory.getLogger(DoubleValueMonitor.class);

    private Session session;
    private String iotdbHost;
    private String user = "root";
    private String password = "root";
    private String targetDevice = "root.ln.alerting";
    private Double lowLevel = 50.0;
    private Double highLevel = 100.0;

    private AtomicLong timestamp = new AtomicLong(new Date().getTime());

    private List<String> measuraments = new ArrayList<>(2);
    private List<TSDataType> tsDataTypes = new ArrayList<>(2);



    @Override
    public void onCreate(TriggerAttributes attributes) throws Exception {
        LOGGER.info("############# trigger onCreate ############");
        if (attributes != null) {
            if (attributes.hasAttribute("remote_ip")) {
                this.iotdbHost = attributes.getString("remote_ip");
            } else {
                throw new RuntimeException("remote_ip is required");
            }
            if (attributes.hasAttribute("lo")) {
                this.lowLevel = Double.parseDouble(attributes.getString("lo"));
            }
            if (attributes.hasAttribute("hi")) {
                this.highLevel = Double.parseDouble(attributes.getString("hi"));
            }
            if (attributes.hasAttribute("user")) {
                this.user = attributes.getString("user");
            }
            if (attributes.hasAttribute("password")) {
                this.password = attributes.getString("password");
            }
        }
        this.measuraments.add("table_name");
        this.measuraments.add("alert_content");
        this.tsDataTypes.add(TSDataType.TEXT);
        this.tsDataTypes.add(TSDataType.TEXT);
    }

    @Override
    public void onDrop() throws IoTDBConnectionException {
        LOGGER.info("############# trigger onDrop ############");
        if (session != null) {
            session.close();
        }
    }
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
    public boolean fire(Tablet tablet) throws Exception {
        ensureSession();
//        LOGGER.info("############# trigger fire ############");
        List<MeasurementSchema> measurementSchemaList = tablet.getSchemas();
        for (int col = 0, n = measurementSchemaList.size(); col < n; col++) {
            if (measurementSchemaList.get(col).getType().equals(TSDataType.DOUBLE)) {
                double[] values = (double[]) tablet.values[col];
                for (int row = 0; row < values.length; row++) {
                    writeOut(row, col, tablet);
                }
            }
        }
        return true;
    }
    private synchronized void writeOut(int row, int col, Tablet tablet) throws IoTDBConnectionException, StatementExecutionException {
        double value = ((double[]) tablet.values[col])[row];
//        LOGGER.info("***************"+value);
        if (value > this.highLevel) {
            List<Object> content = new ArrayList<>(2);
//            LOGGER.info(value+"********* greater than higher value ********" + timestamp.get());
            content.add(tablet.deviceId);
            content.add("[" + tablet.timestamps[row] + "] CRITICAL [" + tablet.getSchemas().get(col).getMeasurementId() + "]: " + value + " greater then " + this.highLevel);
            this.session.insertAlignedRecord(this.targetDevice, timestamp.addAndGet(3), measuraments, tsDataTypes, content);
        } else if (value > lowLevel) {
            List<Object> content = new ArrayList<>(2);
//            LOGGER.info(value+"********* greater than lower value ********" + timestamp.get());
            content.add(tablet.deviceId);
            content.add("[" + tablet.timestamps[row] + "] WARN [" + tablet.getSchemas().get(col).getMeasurementId() + "]: " + value + " greater then " + this.lowLevel);
            this.session.insertAlignedRecord(this.targetDevice, timestamp.addAndGet(2), measuraments, tsDataTypes, content);
        }
    }

}

