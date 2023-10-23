package com.timecho.clients.java;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.*;
import java.util.concurrent.*;

import static java.lang.System.out;

/**
 * 3 个客户端并发写入
 * 1000 ～ 200000 行
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(3);

        pool.execute(new SessionClientRunnable("172.20.70.44", 0L));
        pool.execute(new SessionClientRunnable("172.20.70.45", 2000000L));
        pool.execute(new SessionClientRunnable("172.20.70.46", 4000000L));


        pool.awaitTermination(20, TimeUnit.SECONDS);
        pool.shutdown();
    }
}

class SessionClientRunnable implements Runnable {
    private static final String ROOT_SG1_D1 = "root.multi.d1";
    private String host;
    private long timestamp;
    // 写入行数
    private int maxCount = 200;

    public SessionClientRunnable(String host, long timestamp){
        this.host = host;
        this.timestamp = timestamp;
    }

    @Override
    public void run()  {
        out.println(Thread.currentThread().getName() +"正在执行。。。");
        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema("s1", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("s3", TSDataType.DOUBLE));

        Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 2000000);

        Session session = new Session.Builder().host(host).port(6667).build();
        try {
            session.open(false);
            for (int row = 0; row < this.maxCount; row++) {
                int rowIndex = tablet.rowSize++;
                tablet.addTimestamp(rowIndex, timestamp);
                for (int s = 0; s < 3; s++) {
                    double value = new Random().nextDouble() * 730;
                    tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
                }
                timestamp++;
            }
            out.println("max:"+tablet.getMaxRowNumber());
            out.println(timestamp);

            session.insertTablet(tablet);
        } catch (Exception e) {
            out.println(e);
        } finally {
            try {
                session.close();
            } catch (IoTDBConnectionException e) {
            }
        }
    }
}