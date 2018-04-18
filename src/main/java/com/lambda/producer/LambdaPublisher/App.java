package com.lambda.producer.LambdaPublisher;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Kafka Publisher
 */
public class App {
    private final static String TOPIC = "lambda-demo";
    private final static String BROKERS = "localhost:9092";

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERS);
        props.put("client.id", "LambdaProducer");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(props);
    }

    private static Authentication authorize() {
        String consumerKey = "EcXXXXXXXXXX";
        String consumerSecret = "ZWNKFBDAKXXXXXXXXXXXXXXXXXXXXX";
        String token = "1043941XXXXXXXXXXXXXXXXXX";
        String secret = "U10bXXXXXXXXXXXXXXXXXXXXXXXX";
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
                secret);
        return auth;
    }

    static void runProducer(final String term) throws Exception {
        final KafkaProducer<String, String> producer = createProducer();

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        String[] terms = term.split(",");
        endpoint.trackTerms(Arrays.asList(terms));

        // Authorize
        Authentication auth = authorize();

        // Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();

        // Establish a connection
        client.connect();
        try {
            for (int msg = 0; msg <= 1000; msg++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                        TOPIC, Integer.toString(msg), queue.take());
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

        client.stop();
    }

    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            runProducer("#trending");
        } else {
            runProducer(args[0]);
        }
    }
}
