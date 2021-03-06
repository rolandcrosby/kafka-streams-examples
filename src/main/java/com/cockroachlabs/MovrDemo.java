package com.cockroachlabs;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.map.SingletonMap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;

public class MovrDemo {
    public static void main(final String[] args) throws Exception {
        final String bootstrapServers, schemaRegistry, topic;
        if (args.length < 3) {
            System.out.println("Connecting to Kafka and schema registry on localhost.");
            System.out.println("Arguments: kafkaurl schemaregistry topic");
            bootstrapServers = "localhost:9092";
            schemaRegistry = "http://localhost:8081";
            topic = "tls_sasl_avro_rides";
        } else {
            bootstrapServers = args[0];
            schemaRegistry = args[1];
            topic = args[2];
        }
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "crdb-cdc-consumer");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "crdb-cdc-consumer-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, ClusterLogicalTimestampExtractor.class);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);

        final StreamsBuilder builder = new StreamsBuilder();

        final String stateStorePrefix = "latest_timestamp_";
        final String exactlyOncePrefix = "exactly_once_";
        final String timestampStateStoreName = stateStorePrefix + topic;

        final Serde<GenericRecord> stateKeySerde = new GenericAvroSerde();
        stateKeySerde.configure(new SingletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry), true);
        final StoreBuilder<KeyValueStore<GenericRecord, String>> tsStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(timestampStateStoreName),
                stateKeySerde,
                Serdes.String()
        );
        builder.addStateStore(tsStoreBuilder);

        final KStream<GenericRecord, GenericRecord> inStream = builder.stream(topic);
        inStream.flatTransform(() -> new CRDBExactlyOnceTransformer(timestampStateStoreName), timestampStateStoreName)
                .to(exactlyOncePrefix + topic);

        final Conversion<BigDecimal> conv = new Conversions.DecimalConversion();
        final Schema byteSchema = Schema.create(Schema.Type.BYTES);
        final LogicalType decType = LogicalTypes.decimal(10, 0);
        final KStream<GenericRecord, GenericRecord> exactlyOnceStream = builder.stream(exactlyOncePrefix + topic);
        final KGroupedStream<String, Long> ridesByCity = exactlyOnceStream.flatMap((GenericRecord k, GenericRecord v) -> {
            if (k == null) return Collections.EMPTY_LIST;
            final GenericRecord rec = (GenericRecord) v.get("after");
            final String city = ((Utf8) rec.get("city")).toString();
            final ByteBuffer revBytes = (ByteBuffer) rec.get("revenue");
            if (revBytes == null) {
                return Collections.singleton(new KeyValue<>(city, -1L));
            } else {
                final BigDecimal revenue = conv.fromBytes(revBytes, byteSchema, decType);
                return Collections.singleton(new KeyValue<>(city, revenue.longValue()));
            }
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.Double()));
        final KTable<String, Long> activeRides = ridesByCity.aggregate(
                () -> 0L,
                (city, revenue, acc) -> revenue == -1L ? acc + 1 : acc - 1,
                Materialized.as("active-rides-by-city")
                        .with(Serdes.String(), Serdes.Long())
        );
        final KTable<String, Long> revenueByCity = ridesByCity.aggregate(
                () -> 0L,
                (city, revenue, acc) -> revenue == -1L ? acc : acc + revenue,
                Materialized.as("revenue-by-city")
                        .with(Serdes.String(), Serdes.Long())
        );
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);


        final HttpServer server = HttpServer.create(new InetSocketAddress(7001), 0);
        server.createContext("/", new StatsHandler(streams));
        server.setExecutor(null);

        streams.cleanUp();
        streams.start();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(0);
            streams.close();
        }));
    }

    static class StatsHandler implements HttpHandler {
        private final KafkaStreams streams;
        private ReadOnlyKeyValueStore<String, Long> activeRidesStore;
        private ReadOnlyKeyValueStore<String, Long> revenueStore;

        public StatsHandler(final KafkaStreams streams) {
            this.streams = streams;
        }

        private void init() {
        }

        @Override
        public void handle(final HttpExchange ex) throws IOException {
            if (this.activeRidesStore == null || this.revenueStore == null) {
                this.activeRidesStore = streams.store("active-rides-by-city", QueryableStoreTypes.keyValueStore());
                this.revenueStore = streams.store("revenue-by-city", QueryableStoreTypes.keyValueStore());
            }
            final Headers headers = ex.getResponseHeaders();
            headers.set("Content-type", "text/html; charset=utf-8");
            String response = "<style>body{font-family: sans-serif;}</style>";
            response += "<h2>Rides by City</h2><ul>";
            final KeyValueIterator<String, Long> ridesRange = activeRidesStore.all();
            while (ridesRange.hasNext()) {
                final KeyValue<String, Long> next = ridesRange.next();
                response = response + "<li>" + next.key + ": " + next.value + "</li>";
            }
            ridesRange.close();
            response += "</ul><h2>Revenue by City</h2><ul>";
            final KeyValueIterator<String, Long> revenueRange = activeRidesStore.all();
            while (revenueRange.hasNext()) {
                final KeyValue<String, Long> next = revenueRange.next();
                response = response + "<li>" + next.key + ": " + next.value + "</li>";
            }
            revenueRange.close();
            response += "</ul>";
            ex.sendResponseHeaders(200, response.length());
            final OutputStream os = ex.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}
