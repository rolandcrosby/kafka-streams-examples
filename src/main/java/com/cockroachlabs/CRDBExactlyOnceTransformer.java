package com.cockroachlabs;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;

public class CRDBExactlyOnceTransformer implements ValueTransformerWithKey<GenericRecord, GenericRecord, GenericRecord> {
    private ProcessorContext processorContext;
    private String stateStoreName;
    private KeyValueStore<GenericRecord, String> state;
    private static GenericRecord fakeAvroNull;

    public CRDBExactlyOnceTransformer(final String stateStoreName) {
        this.stateStoreName = stateStoreName;
        if (fakeAvroNull == null) {
            final Schema fakeNullRecordSchema = Schema.createRecord("__crdb_null__", "fake null record", "", false);
            fakeNullRecordSchema.setFields(Collections.singletonList(
                    new Schema.Field("__crdb_null__", Schema.create(Schema.Type.NULL), "fake null", JsonProperties.NULL_VALUE))
            );
            fakeAvroNull = new GenericRecordBuilder(fakeNullRecordSchema).build();
        }
    }

    @Override
    public void init(final ProcessorContext processorContext) {
        this.processorContext = processorContext;
        this.state = (KeyValueStore) this.processorContext.getStateStore(stateStoreName);
    }

    @Override
    public GenericRecord transform(final GenericRecord key, final GenericRecord value) {
        Utf8 ts = (Utf8) value.get("updated");
        if (ts == null) {
            ts = (Utf8) value.get("resolved");
            if (ts == null) {
                throw new RuntimeException("timestamp not found in input message");
            }
        }

        GenericRecord k = key;
        if (k == null) {
            k = fakeAvroNull;
        }

        final String tsString = ts.toString();
        String lastTS;
        try {
            lastTS = this.state.get(k);
        } catch (final NullPointerException e) {
            lastTS = null;
        }
        if (lastTS == null || tsString.compareTo(lastTS) > 0) {
            System.out.printf("UPDATE: %s\tts %s > %s\n", k, tsString, lastTS);
            this.state.put(k, tsString);
            return value;
        }
        System.out.printf("IGNORE: %s\tts %s <= %s\n", k, tsString, lastTS);
        return null;
    }

    @Override
    public void close() {

    }
}

