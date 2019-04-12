package com.cockroachlabs;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ClusterLogicalTimestampExtractor implements org.apache.kafka.streams.processor.TimestampExtractor {
    @Override
    public long extract(final ConsumerRecord<Object, Object> consumerRecord, final long l) {
        final GenericRecord value = (GenericRecord) consumerRecord.value();
        String ts;
        ts = (String) value.get("resolved");
        if (ts == null) {
            ts = (String) value.get("updated");
            if (ts == null) {
                throw new RuntimeException("can't find resolved or updated timestamp");
            }
        }

        final int decimal = ts.indexOf(".");
        return Integer.parseInt(ts.substring(0, decimal - 6));
    }
}
