package com.cockroachlabs;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ClusterLogicalTimestampExtractor implements org.apache.kafka.streams.processor.TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        GenericRecord value = (GenericRecord) consumerRecord.value();
        String ts;
        try {
            ts = (String) value.get("resolved");
        } catch (Exception e) {
            ts = (String) value.get("updated");
        }
        int decimal = ts.indexOf(".");
        return Integer.parseInt(ts.substring(0, decimal - 6));
    }
}
