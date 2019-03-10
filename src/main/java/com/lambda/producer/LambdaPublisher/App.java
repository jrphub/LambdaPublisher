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
 *
 * Publishing tweets in real time
 *
 */
public class App {
    //default values
    private static String brokers = "localhost:9092";
    private static String groupId = "tweet-publisher";
    private static String topic = "tweets-ml-demo";
    private static String keyword = "#trending";

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("client.id", groupId);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(props);
    }

    private static Authentication authorize() {
        /*String consumerKey = "EcXXXXXXXXXX";
        String consumerSecret = "ZWNKFBDAKXXXXXXXXXXXXXXXXXXXXX";
        String token = "1043941XXXXXXXXXXXXXXXXXX";
        String secret = "U10bXXXXXXXXXXXXXXXXXXXXXXXX";*/

        String consumerKey = "EcL6OsfBE9FrFaW6KcH3uUQ8J";
        String consumerSecret = "ZWNKFBDAKay4DXqdoIUafhiKGQUoa2kdmWI1NH8nSgwCcTYOke";
        String token = "104394125-YiXvzWIBnufh3OF99XfG3KAY9MmbOiBNGHCSM913";
        String secret = "U10bCov5D5z70R2oaRdiWsteLXaS3HQGTW6Yx3e5jUt15";

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
                        topic, Integer.toString(msg), queue.take());
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
        System.out.println("Pulling data for " + args[0]);
        if (args.length == 3) {
            brokers = args[0];
            topic = args[1];
            keyword = args[2];
        }
        runProducer(keyword);
    }
}
