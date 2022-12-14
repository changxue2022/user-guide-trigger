
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 对于各种类型监控：
 * 数值型 > 400
 * text 类型的字符串长度>10
 * boolean 类型不为真
 * 支持同一device的多列
 */
public class TriggerTest implements Trigger {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerTest.class);

  private static final String TARGET_DEVICE = "root.target.alerting";
  private String remote_ip;
  private String ts_type="double";
  private final double standardValue = 400.0;
  private String trig_name="default_test";
  private Session session;
  private List<String> measuraments = new ArrayList<>(4);
  private List<TSDataType> tsDataTypes = new ArrayList<>(4);
  private List<Object> content = new ArrayList<>(4);

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    LOGGER.info("********* onCreate *********["+trig_name+"]");
    if (attributes.hasAttribute("remote_ip")) {
        remote_ip = attributes.getString("remote_ip");
    } else {
        throw new RuntimeException("remote_ip is required");
    }
    ts_type = attributes.getString("ts_type").toLowerCase();
    trig_name = attributes.getString("trig_name");

      this.measuraments.add("trig_name");
      this.measuraments.add("device");
      this.measuraments.add("ts_type");
      this.measuraments.add("value");
      this.tsDataTypes.add(TSDataType.TEXT);
      this.tsDataTypes.add(TSDataType.TEXT);
      this.tsDataTypes.add(TSDataType.TEXT);
      this.tsDataTypes.add(TSDataType.TEXT);
  }
    private void ensureSession() throws IoTDBConnectionException {
        if (session == null ) {
            session = new Session.Builder()
                    .host(remote_ip)
                    .build();
            session.open(false);
        }
    }

  @Override
  public boolean fire(Tablet tablet) throws Exception {
      LOGGER.info("###########  fire #############  ["+trig_name+"]");
      ensureSession();
      List<MeasurementSchema> measurementSchemaList = tablet.getSchemas();
      for (int i = 0, n = measurementSchemaList.size(); i < n; i++) {
          switch (ts_type) {
              case "double":
                  if (measurementSchemaList.get(i).getType().equals(TSDataType.DOUBLE)) {
                      // for example, we only deal with the columns of Double type
                      double[] values = (double[]) tablet.values[i];
                      for (double value : values) {
                          if ( value > this.standardValue ) {
                              this.content.add(trig_name);
                              this.content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                              this.content.add(ts_type);
                              this.content.add("["+tablet.timestamps[0]+"]"+value);
                              this.session.insertAlignedRecord(TARGET_DEVICE, new Date().getTime(), measuraments, tsDataTypes, content);
                              this.content.clear();
                          }
                      }
                  }
                  continue;
              case "int32":
                  if (measurementSchemaList.get(i).getType().equals(TSDataType.INT32)) {
                      // for example, we only deal with the columns of Double type
                      int[] values = (int[]) tablet.values[i];
                      for (int value : values) {
                          if ( value > this.standardValue ) {
                              this.content.add(trig_name);
                              this.content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                              this.content.add(ts_type);
                              this.content.add("["+tablet.timestamps[0]+"]"+value);
                              this.session.insertAlignedRecord(TARGET_DEVICE, new Date().getTime(), measuraments, tsDataTypes, content);
                              this.content.clear();
                          }
                      }
                  }
                  continue;
              case "int64":
                  if (measurementSchemaList.get(i).getType().equals(TSDataType.INT64)) {
                      // for example, we only deal with the columns of Double type
                      long[] values = (long[]) tablet.values[i];
                      for (long value : values) {
                          if ( value > this.standardValue ) {
                              this.content.add(trig_name);
                              this.content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                              this.content.add(ts_type);
                              this.content.add("["+tablet.timestamps[0]+"]"+value);
                              this.session.insertAlignedRecord(TARGET_DEVICE, new Date().getTime(), measuraments, tsDataTypes, content);
                              this.content.clear();
                          }
                      }
                  }
                  continue;
              case "float":
                  if (measurementSchemaList.get(i).getType().equals(TSDataType.FLOAT)) {
                      // for example, we only deal with the columns of Double type
                      float[] values = (float[]) tablet.values[i];
                      for (float value : values) {
                          if ( value > this.standardValue ) {
                              this.content.add(trig_name);
                              this.content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                              this.content.add(ts_type);
                              this.content.add("["+tablet.timestamps[0]+"]"+value);
                              this.session.insertAlignedRecord(TARGET_DEVICE, new Date().getTime(), measuraments, tsDataTypes, content);
                              this.content.clear();
                          }
                      }
                  }
                  continue;
              case "boolean":
                  if (measurementSchemaList.get(i).getType().equals(TSDataType.BOOLEAN)) {
                      // for example, we only deal with the columns of Double type
                      boolean[] values = (boolean[]) tablet.values[i];
                      for (boolean value : values) {
                          if ( ! value ) {
                              this.content.add(trig_name);
                              this.content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                              this.content.add(ts_type);
                              this.content.add("["+tablet.timestamps[0]+"] false");
                              this.session.insertAlignedRecord(TARGET_DEVICE, new Date().getTime(), measuraments, tsDataTypes, content);
                              this.content.clear();
                          }
                      }
                  }
                  continue;
              case "text":
                  if (measurementSchemaList.get(i).getType().equals(TSDataType.TEXT)) {
                      // for example, we only deal with the columns of Double type
                      Binary[] values = (Binary[]) tablet.values[i];
                      for (Binary value : values) {
                          LOGGER.info("####### TEXT: "+value+"]");
                          if ( value != null && value.toString().length() > 10) {
                              LOGGER.info("********** enter");
                              this.content.add(trig_name);
                              this.content.add(tablet.deviceId+"."+tablet.getSchemas().get(i).getMeasurementId());
                              this.content.add(ts_type);
                              this.content.add("["+tablet.timestamps[0]+"]"+value.toString());
                              this.session.insertAlignedRecord(TARGET_DEVICE, new Date().getTime(), measuraments, tsDataTypes, content);
                              this.content.clear();
                          }
                      }
                  }
                  continue;
              default:
                  LOGGER.error("Invalid datatype.");
          }
      }
      return true;
  }

    @Override
    public void restore() throws Exception {
        LOGGER.info("###### restore ########## ["+trig_name+"]");
    }

    @Override
  public void onDrop() throws Exception {
    LOGGER.info("********** onDrop() *********** ["+trig_name+"]");
        if (session != null) {
            session.close();
        }
    }

    @Override
    public FailureStrategy getFailureStrategy() {
        return FailureStrategy.PESSIMISTIC;
    }
}

