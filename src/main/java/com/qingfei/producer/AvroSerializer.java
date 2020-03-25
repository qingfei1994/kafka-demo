package com.qingfei.producer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;



public class AvroSerializer implements Serializer<User> {

    String schemaString = "{\"namespace\":\"com.qingfei.producer\",\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"age\",\"type\":\"int\"}] }";


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, User user) {
        Schema schema = new Schema.Parser().parse(schemaString);
        DatumWriter<User> writer = new SpecificDatumWriter<>(User.class);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<User> fileWriter = new DataFileWriter<>(writer);
        try {
            fileWriter.create(schema,baos);
            fileWriter.append(user);
            return baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                baos.close();
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;

    }

    @Override
    public void close() {

    }
}
