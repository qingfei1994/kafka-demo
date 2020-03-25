package com.qingfei.producer;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

public class User extends SpecificRecordBase implements SpecificRecord {

    public static final Schema SCHEMA$ =
            new Schema.Parser().parse("{\"namespace\":\"com.qingfei.producer\",\"type\":\"record\",\"name\":\"User\",\"fields\":[" +
            "{\"name\":\"name\",\"type\":\"string\"}," +
            "{\"name\":\"age\",\"type\":\"int\"}] }");
    private String name;
    private Integer age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public Object get(int field) {
        switch (field){
            case 0: return this.name;
            case 1: return this.age;
            default: throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0: this.setName((String)value);
            case 1: this.setAge((Integer) value);
            default:throw new org.apache.avro.AvroRuntimeException("Bad index");
        }
    }
}
