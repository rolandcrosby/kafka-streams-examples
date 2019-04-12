package com.cockroachlabs;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ClusterLogicalTimestampExtractor implements org.apache.kafka.streams.processor.TimestampExtractor {
    @Override
    public long extract(final ConsumerRecord<Object, Object> consumerRecord, final long l) {
        final GenericRecord value = (GenericRecord) consumerRecord.value();
        Utf8 ts;
        ts = (Utf8) value.get("resolved");
        if (ts == null) {
            ts = (Utf8) value.get("updated");
            if (ts == null) {
                throw new RuntimeException("can't find resolved or updated timestamp");
            }
        }
        final String tsString = ts.toString();
        final int decimal = tsString.indexOf(".");
        return Integer.parseInt(tsString.substring(0, decimal - 6));
    }
}
