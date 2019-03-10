package com.lambda.producer.avroProducer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 *
 * Sample Code for Avro Producer
 */

public class AvroProducer {
    private final static String TOPIC = "avro-producer";
    private final static String BROKERS = "localhost:9092";

    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\" },"
            + "  { \"name\":\"str2\", \"type\":\"string\" }"
            + "]}";

    /*
     * create a topic using kafka command-line utility
     *
     * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic avro-producer
     *
     * */
    private static KafkaProducer<String, byte[]> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERS);
        props.put("client.id", "LambdaPublisher");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(props);
    }

    private static void runProducer() throws Exception {
        final KafkaProducer<String, byte[]> producer = createProducer();
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        long time = System.currentTimeMillis();

        try {
            for (int i = 0; i < 1000; i++) {
                GenericData.Record avroRecord = new GenericData.Record(schema);
                avroRecord.put("str1", "Str 1-" + i);
                avroRecord.put("str2", "Str 2-" + i);

                byte[] bytes = recordInjection.apply(avroRecord);

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, bytes);
                producer.send(record);

                Thread.sleep(250);

            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String... args) throws Exception {
        runProducer();
    }

    /*
    * Sample output :
    *
    *   Str 1-190Str 2-190
        Str 1-191Str 2-191
        Str 1-192Str 2-192
        Str 1-193Str 2-193
        Str 1-194Str 2-194
        Str 1-195Str 2-195
        Str 1-196Str 2-196

    * */
}