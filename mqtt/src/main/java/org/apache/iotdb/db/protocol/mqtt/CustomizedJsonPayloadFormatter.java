package org.apache.iotdb.db.protocol.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CustomizedJsonPayloadFormatter implements PayloadFormatter {

    @Override
    public List<Message> format(ByteBuf payload) {
        // Suppose the payload is a json format
        if (payload == null) {
            return null;
        }

        List<Message> ret = new ArrayList<>();
        String json = payload.toString(StandardCharsets.UTF_8);
        // parse data from the json and generate Messages and put them into List<Meesage> ret
        ObjectMapper obj = new ObjectMapper();
        Reader reader = new StringReader(json);
        try {
            PayloadJson node = obj.readValue(reader, PayloadJson.class);
            List<TSDataType> dataTypes = new ArrayList<>();
            dataTypes.add(TSDataType.FLOAT);

            Message message = new Message();
            message.setDevice(node.getDevice());
            message.setTimestamp(node.getTimestamp());
            message.setDataTypes(dataTypes);
            message.setMeasurements(node.getMeasurements());
            message.setValues(node.getValues());
            ret.add(message);
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public String getName() {
        // set the value of mqtt_payload_formatter in iotdb-datanode.properties as the following string:
        return "CustomizedJson";
    }

    public static void main(String[] args) throws IOException {
        // parse json string
        String json = String.format("{\n" +
                "\"device\":\"%s\",\n" +
                "\"timestamp\":%d,\n" +
                "\"measurements\":[\"s1\"],\n" +
                "\"values\":[%d]\n" +
                "}", "root.sg.d0", 100, 1);
        ObjectMapper obj = new ObjectMapper();
        Reader reader = new StringReader(json);
        PayloadJson node = obj.readValue(reader, PayloadJson.class);
        System.out.println(node.getDevice());
        System.out.println(node.getTimestamp());
        System.out.println(node.getValues());

    }
}