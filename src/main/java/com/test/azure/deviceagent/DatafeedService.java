package com.test.azure.deviceagent;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DatafeedService {

    private static final Logger logger = LoggerFactory.getLogger(DatafeedService.class);

    private Properties props = new Properties();

    private KafkaProducer<String,String> producer = null;

    private String topicName = null;

    public DatafeedService(String topicName, String host, String port) {
        this.topicName = topicName;
        String bootstrapServerConfig = host + ":" + port;

        //Refer to http://kafka.apache.org/documentation.html#producerconfigs for config details
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        logger.debug("Initializing Kafka Producer with: BootstrapServerConfig={} Topic={}", bootstrapServerConfig, topicName);

        producer = new KafkaProducer<String,String>(props);
    }

    public void publish(final String data) {

        logger.debug("Sending data to Kafka : {}", data);

        long timestamp = System.currentTimeMillis();
        producer.send(new ProducerRecord<String, String>(topicName, "data", data), (recordMetadata, e) -> {
            if (e != null) {
                logger.error("Got Error ", e);
            } else if (recordMetadata != null && recordMetadata.topic() != null) {
                logger.info("Successfully send data to " + recordMetadata.topic());
            }
        });
        timestamp = System.currentTimeMillis() - timestamp;
        logger.debug("Successfully queued data to Kafka in {} milliseconds", timestamp);
    }

}
