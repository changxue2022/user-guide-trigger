package com.timecho.clients.mqtt;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

public class MqttClients {
    public static final String alignedDevice = "root.stateful.mqtt";
    public static final String nonAlignedDevice = "root.stateless.mqtt";

    public static void main(String[] args) throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setHost("172.20.70.44", 1883);
        mqtt.setUserName("root");
        mqtt.setPassword("root");

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        Long timstampBase = System.currentTimeMillis();
        Double valueBase = 700.0;


        for (int i = 0; i < 3; i++) {
            String payload = String.format("{\n" +
                    "\"device\":\"%s\",\n" +
                    "\"timestamp\":%d,\n" +
                    "\"measurements\":[\"s1\"],\n" +
                    "\"values\":[%f]\n" +
                    "}", alignedDevice, timstampBase+i, valueBase+i*5);

            connection.publish(alignedDevice+".s1", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
        }
//        for (int i = 0; i < 5; i++) {
//            String payload = String.format("{\n" +
//                    "\"device\":\"%s\",\n" +
//                    "\"timestamp\":%d,\n" +
//                    "\"measurements\":[\"s1\"],\n" +
//                    "\"values\":[%f]\n" +
//                    "}", nonAlignedDevice, timstampBase+i*2, valueBase+i*6);
//
//            connection.publish(nonAlignedDevice+".s1", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
//        }

        connection.disconnect();
    }
}
