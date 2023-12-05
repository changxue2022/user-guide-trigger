
import org.apache.iotdb.session.Session;
import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 对于各种类型监控：
 * 数值型 > 74000000
 * text 类型的字符串长度>65536
 * boolean 类型为null
 * 支持同一device的多列
 */
public class TriggerTestPerf implements Trigger {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerTestPerf.class);
  private String ts_type="double";
  private final double standardValue = 74000000.0;
  private String trig_name="default_test";
  private Session session;

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    LOGGER.info("********* onCreate *********["+trig_name+"]");
    ts_type = attributes.getString("ts_type").toLowerCase();
    trig_name = attributes.getString("trig_name");

  }

  @Override
  public boolean fire(Tablet tablet) throws Exception {
//      LOGGER.info("#####  fire ##### [{}] {}", tablet.timestamps[0],trig_name);
      List<MeasurementSchema> measurementSchemaList = tablet.getSchemas();
      for (int col = 0, n = measurementSchemaList.size(); col < n; col++) {
          switch (ts_type) {
              case "double":
                  if (measurementSchemaList.get(col).getType().equals(TSDataType.DOUBLE)) {
                      // for example, we only deal with the columns of Double type
                      double[] values = (double[]) tablet.values[col];
                      for (int row = 0; row < values.length; row++) {
                         if (values[row] > standardValue) {
                             break;
                         }
                      }
                  }
                  continue;
              case "int32":
                  if (measurementSchemaList.get(col).getType().equals(TSDataType.INT32)) {
                      // for example, we only deal with the columns of Double type
                      int[] values = (int[]) tablet.values[col];
                      for (int row = 0; row < values.length; row++) {
                          if (values[row] > standardValue) {
                              break;
                          }
                      }
                  }
                  continue;
              case "int64":
                  if (measurementSchemaList.get(col).getType().equals(TSDataType.INT64)) {
                      // for example, we only deal with the columns of Double type
                      long[] values = (long[]) tablet.values[col];
                      for (int row = 0; row < values.length; row++) {
                          if (values[row] > standardValue) {
                              break;
                          }
                      }
                  }
                  continue;
              case "float":
                  if (measurementSchemaList.get(col).getType().equals(TSDataType.FLOAT)) {
                      // for example, we only deal with the columns of Double type
                      float[] values = (float[]) tablet.values[col];
                      for (int row = 0; row < values.length; row++) {
                          if (values[row] > standardValue) {
                              break;
                          }
                      }
                  }
                  continue;
              case "boolean":
                  if (measurementSchemaList.get(col).getType().equals(TSDataType.BOOLEAN)) {
                      BitMap bitMap = null;
                      if(tablet.bitMaps != null){
                          bitMap = tablet.bitMaps[col];
                      }
                      boolean[] values = (boolean[]) tablet.values[col];
                      for(int row=0;row<values.length;row++){
                          if(bitMap != null && bitMap.isMarked(row)) {// process null
                              break;
                          }
                      }
                  }
                  continue;
              case "text":
                  if (measurementSchemaList.get(col).getType().equals(TSDataType.TEXT)) {
                      // for example, we only deal with the columns of Double type
                      Binary[] values = (Binary[]) tablet.values[col];
                      for (int row = 0; row < values.length; row++) {
                          if (values[row].toString().length() > standardValue) {
                              break;
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
    }

    @Override
    public FailureStrategy getFailureStrategy() {
        return FailureStrategy.PESSIMISTIC;
    }
}

