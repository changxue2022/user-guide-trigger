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

/**
 * 监控某列值，写出大于standard
 * 写入root.ln.alerting device中
 * 支持一个device的多列
 */
public class DoubleValueSimpleMonitor implements Trigger {
    private static final Logger LOGGER = LoggerFactory.getLogger(DoubleValueSimpleMonitor.class);

    private Session session;
    private String iotdbHost;
    private String targetDevice = "root.ln.alerting";
    private Double standardValue = 50.0;

    private List<String> measuraments = new ArrayList<>(2);
    private List<TSDataType> tsDataTypes = new ArrayList<>(2);
    private List<Object> content = new ArrayList<>(2);

    private void ensureSession() throws IoTDBConnectionException {
        if (session == null ) {
            session = new Session.Builder()
                    .host(iotdbHost)
                    .build();
            session.open(false);
        }
    }

    @Override
    public void onCreate(TriggerAttributes attributes) throws Exception {
//        LOGGER.info("############# trigger onCreate ############");
        if (attributes != null) {
            if (attributes.hasAttribute("remote_ip")) {
                this.iotdbHost = attributes.getString("remote_ip");
            } else {
                throw new RuntimeException("remote_ip is required");
            }
            if (attributes.hasAttribute("standard_value")) {
                this.standardValue = Double.parseDouble(attributes.getString("standard_value"));
            }
        }
        this.measuraments.add("table_name");
        this.measuraments.add("time_org");
        this.measuraments.add("value");
        this.tsDataTypes.add(TSDataType.TEXT);
        this.tsDataTypes.add(TSDataType.INT64);
        this.tsDataTypes.add(TSDataType.DOUBLE);
    }

    @Override
    public void onDrop() throws IoTDBConnectionException {
//        LOGGER.info("############# trigger onDrop ############");
        if (this.session != null) {
            this.session.close();
        }
    }

    @Override
    public boolean fire(Tablet tablet) throws Exception {
//        LOGGER.info("############# trigger fire ############ "+tablet.timestamps);
        ensureSession();
        List<MeasurementSchema> measurementSchemaList = tablet.getSchemas();
        for (int col = 0, n = measurementSchemaList.size(); col < n; col++) {
            if (measurementSchemaList.get(col).getType().equals(TSDataType.DOUBLE)) {
                double[] values = (double[]) tablet.values[col];
                for (int row =0; row < values.length; row++) {
                    writeOut(row, col, tablet);
                }
            }
        }
        return true;
    }
    private synchronized void writeOut(int row, int col, Tablet tablet) throws IoTDBConnectionException, StatementExecutionException {
        double value = ((double[]) tablet.values[col])[row];
        if (value > this.standardValue) {
            this.content.add(tablet.deviceId + "." + tablet.getSchemas().get(col).getMeasurementId());
            this.content.add(tablet.timestamps[row]);
            this.content.add(value);
            this.session.insertAlignedRecord(this.targetDevice, new Date().getTime(), measuraments, tsDataTypes, content);
            this.content.clear();
        }
    }
}