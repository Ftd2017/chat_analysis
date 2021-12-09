package com.closer.prophet;

import com.closer.prophet.handler.DefaultHandler;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Configuring application and submitting to Apache spark.
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    // configurations.
//    private static final String zkHost = "localhost:2181";
//    private static final String brokerList = "localhost:9092";
//    private static final String topic = "test";
    private static final String zkHost = "192.168.0.142:2181,192.168.0.144:2181,192.168.0.143:2181";
    private static final String brokerList = "192.168.0.142:9092,192.168.0.144:9092,192.168.0.143:9092";
    private static final String topic = "user_action";
    private static final String groupId = "log-consumer";

    public static void main(String[] args) throws IOException {
        DefaultHandler defaultHandler = new DefaultHandler();

        SparkConf sparkConf = new SparkConf();
//        sparkConf.setAppName("a").setMaster("local[4]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Seconds.apply(10));
        JavaSparkContext sc = ssc.sparkContext();
        sc.setLogLevel("WARN");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(ssc,
                zkHost,
                topic,
                brokerList,
                groupId,
                "",
                true
        );

        try {
            JavaDStream<String> groukLogs;
            groukLogs = kafkaConsumer.createKafkaStream();
            groukLogs.print();
            groukLogs.foreachRDD(defaultHandler::process);
            kafkaConsumer.submit(groukLogs);

            // submit and start application.
            ssc.start();
            ssc.awaitTermination();

        } catch (SparkException e) {
            logger.error(e.getMessage());
        } catch (InterruptedException ie) {
            logger.error("Application is shut down: RECEIVED SIGNAL TERM", ie);
            Thread.currentThread().interrupt();
        }
    }
}
