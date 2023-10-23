//package com.timecho.clients.jdbc;
//
//import java.sql.*;
//import org.apache.iotdb.jdbc.IoTDBSQLException;
//
//public class JDBCClient {
//    private static final String alignedDevice = "root.stateful.jdbc";
//    private static final String nonAlignedDevice = "root.stateless.jdbc";
//
//    public static Connection getConnection() throws ClassNotFoundException, SQLException {
//        String driver = "org.apache.iotdb.jdbc.IoTDBDriver";
//        String url = "jdbc:iotdb://172.20.70.44:6667/";
//        // set rpc compress mode
//        // String url = "jdbc:iotdb://127.0.0.1:6667?rpc_compress=true";
//        String username = "root";
//        String password = "root";
//
//        Class.forName(driver);
//        Connection connection = DriverManager.getConnection(url, username, password);
//
//        return connection;
//    }
//
//    public static void main(String[] args) throws SQLException, ClassNotFoundException {
//        Connection connection = getConnection();
//
//        Statement statement = connection.createStatement();
//        //Create database
//        statement.execute("DROP DATABASE root.stateful");
//        statement.execute("DROP DATABASE root.stateless");
//        statement.execute("CREATE DATABASE root.stateful");
//        statement.execute("CREATE DATABASE root.stateless");
//
//        //Create time series
//        //Different data type has different encoding methods. Here use INT32 as an example
//        statement.execute("CREATE TIMESERIES ? WITH DATATYPE=DOUBLE,ENCODING=plain;", new String[]{nonAlignedDevice+".s1"});
//        statement.execute("create aligned timeseries ? (s1 double)", new String[]{alignedDevice});
//        //Show devices
//        statement.execute("SHOW DEVICES");
//        outputResult(statement.getResultSet());
//        //Count time series
//
//
//        //Execute insert statements in batch
//        statement.execute("insert into ? values (1, 701);", new String[]{nonAlignedDevice});
//        statement.addBatch("insert into "+ nonAlignedDevice +" (timestamp,s1) values(11,1);");
//        statement.addBatch("insert into "+ nonAlignedDevice +" (timestamp,s1) values(12,15);");
//        statement.addBatch("insert into "+ nonAlignedDevice +" (timestamp,s1) values(12,717);");
//        statement.addBatch("insert into "+ nonAlignedDevice +" (timestamp,s1) values(4,712);");
//        statement.executeBatch();
//        statement.clearBatch();
//
//        //Full query statement
//        String sql = "select * from "+nonAlignedDevice;
//        ResultSet resultSet = statement.executeQuery(sql);
//        System.out.println("sql: " + sql);
//        outputResult(resultSet);
//
//        statement.execute("insert into ? (time,s1) aligned values (1, 109);", new String[]{alignedDevice});
//        statement.addBatch("insert into "+alignedDevice+" (time, s1) aligned values (12, 105);");
//        statement.addBatch("insert into "+alignedDevice+" (time, s1) aligned values (3, 15);");
//        statement.addBatch("insert into "+alignedDevice+" (time, s1) aligned values (4, 175);");
//        statement.addBatch("insert into "+alignedDevice+" (time, s1) aligned values (5, 105);");
//
//        sql = "select * from "+alignedDevice;
//        resultSet = statement.executeQuery(sql);
//        System.out.println("sql: " + sql);
//        outputResult(resultSet);
//
//        //Delete time series
//        statement.execute("delete timeseries "+alignedDevice+".s1;");
//        statement.execute("delete timeseries "+nonAlignedDevice+".s1;");
//
//        //close connection
//        statement.close();
//        connection.close();
//        }
//    private static void outputResult(ResultSet resultSet) throws SQLException {
//        if (resultSet != null) {
//            System.out.println("--------------------------");
//            final ResultSetMetaData metaData = resultSet.getMetaData();
//            final int columnCount = metaData.getColumnCount();
//            for (int i = 0; i < columnCount; i++) {
//                System.out.print(metaData.getColumnLabel(i + 1) + " ");
//            }
//            System.out.println();
//            while (resultSet.next()) {
//                for (int i = 1; ; i++) {
//                    System.out.print(resultSet.getString(i));
//                    if (i < columnCount) {
//                        System.out.print(", ");
//                    } else {
//                        System.out.println();
//                        break;
//                    }
//                }
//            }
//            System.out.println("--------------------------\n");
//        }
//    }
//}
