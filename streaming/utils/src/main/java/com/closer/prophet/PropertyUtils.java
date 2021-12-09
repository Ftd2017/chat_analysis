package com.closer.prophet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtils {
    private static Logger logger = LoggerFactory.getLogger(PropertyUtils.class);

    private static Properties appProps = new Properties();
    private static Properties odpsProps = new Properties();
    private static Properties kafkaProps = new Properties();

    PropertyUtils() {}

    private static void loadAppProps() {
        String configPath = System.getProperty("application.config");

        try {
            FileInputStream ins = new FileInputStream(configPath);
            appProps.load(ins);
        } catch (IOException | NullPointerException e) {
            try {
                ClassLoader classLoader = PropertyUtils.class.getClassLoader();
                InputStream ins = classLoader.getResourceAsStream("app.properties");
                appProps.load(ins);
            } catch (IOException ioe) {
                logger.error("Failed to load application configurations!", ioe);
            }
        }
    }

    private static void loadOdpsProps() {
        InputStream ins;

        if (getAppProps().isEmpty()) {
            ClassLoader classLoader = PropertyUtils.class.getClassLoader();
            ins = classLoader.getResourceAsStream("odps/odps.properties");
        } else {
            try {
                String configPath = getAppProps().getProperty("odps.properties");
                ins = new FileInputStream(configPath);
            } catch (IOException | NullPointerException e) {
                ClassLoader classLoader = PropertyUtils.class.getClassLoader();
                ins = classLoader.getResourceAsStream("odps/odps.properties");
            }
        }

        try {
            odpsProps.load(ins);
        } catch (IOException e) {
            logger.error("Failed to load ODPS configurations!", e);
        }
    }

    private static void loadKafkaProps() {
        InputStream ins;

        if (getAppProps().isEmpty()) {
            ClassLoader classLoader = PropertyUtils.class.getClassLoader();
            ins = classLoader.getResourceAsStream("kafka/kafka.properties");
        } else {
            try {
                String configPath = getAppProps().getProperty("kafka.properties");
                ins = new FileInputStream(configPath);
            } catch (IOException | NullPointerException e) {
                ClassLoader classLoader = PropertyUtils.class.getClassLoader();
                ins = classLoader.getResourceAsStream("kafka/kafka.properties");
            }
        }

        try {
            kafkaProps.load(ins);
        } catch (IOException e) {
            logger.error("Failed to load Kafka configurations!", e);
        }
    }

    public static Properties getAppProps() {
        if (appProps.isEmpty()) {
            loadAppProps();
        }
        return appProps;
    }

    public static Properties getOdpsProps() {
        if (odpsProps.isEmpty()) {
            loadOdpsProps();
        }
        return odpsProps;
    }

    public static Properties getKafkaProps() {
        if (kafkaProps.isEmpty()) {
            loadKafkaProps();
        }

        return kafkaProps;
    }


}
