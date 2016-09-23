package com.test.azure.deviceagent;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.function.Consumer;

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static DatafeedService shareInsightDatafeedService;

    public static void main(String[] args) throws IOException {

        if (args.length != 4) {
            logger.info("Invalid Input Params given. Usage eventhubConnectionString kafkaHost kafkaPort kafkaTopic");
            System.exit(0);
        }
        String eventhubConnectionString = args[0];
        String kafkaHost = args[1];
        String kafkaPort = args[2];
        String kafkaTopic = args[3];

        logger.info("=========================================================");
        logger.info("Event Hub Connection String : " + eventhubConnectionString);
        logger.info("Kafka Host                  : " + eventhubConnectionString);
        logger.info("Kafka Port                  : " + eventhubConnectionString);
        logger.info("Kafka Topic                 : " + eventhubConnectionString);
        logger.info("=========================================================");

        shareInsightDatafeedService = new DatafeedService(kafkaTopic, kafkaHost, kafkaPort);

        EventHubClient client0 = receiveMessages("0", eventhubConnectionString);
        EventHubClient client1 = receiveMessages("1", eventhubConnectionString);
        logger.info("Press ENTER to exit.");
        System.in.read();
        try {
            client0.closeSync();
            client1.closeSync();
            System.exit(0);
        } catch (ServiceBusException sbe) {
            System.exit(1);
        }

    }

    private static EventHubClient receiveMessages(final String partitionId, String connStr) {
        EventHubClient client = null;
        try {
            client = EventHubClient.createFromConnectionStringSync(connStr);
        } catch (Exception e) {
            logger.info("Failed to create client: " + e);
            System.exit(1);
        }
        try {
            logger.info("Creating receiver for " + partitionId);
            client.createReceiver(
                    EventHubClient.DEFAULT_CONSUMER_GROUP_NAME,
                    partitionId,
                    Instant.now()).thenAccept(new Consumer<PartitionReceiver>() {
                public void accept(PartitionReceiver receiver) {
                    logger.info("Created receiver on partition " + partitionId);
                    try {
                        while (true) {
                            Iterable<EventData> receivedEvents = receiver.receive(100).get();
                            int batchSize = 0;
                            if (receivedEvents != null) {
                                for (EventData receivedEvent : receivedEvents) {
                                    logger.info(String.format("Offset: %s, SeqNo: %s, EnqueueTime: %s",
                                            receivedEvent.getSystemProperties().getOffset(),
                                            receivedEvent.getSystemProperties().getSequenceNumber(),
                                            receivedEvent.getSystemProperties().getEnqueuedTime()));
                                    logger.info(String.format("| Device ID: %s", receivedEvent.getProperties().get("iothub-connection-device-id")));
                                    logger.info(String.format("| Message Payload: %s", new String(receivedEvent.getBody(),
                                            Charset.defaultCharset())));
                                    logger.info("Publishing to kafka...");
                                    shareInsightDatafeedService.publish(new String(receivedEvent.getBody()));
                                    batchSize++;
                                }
                            }
                            logger.info(String.format("Partition: %s, ReceivedBatch Size: %s", partitionId, batchSize));
                        }
                    } catch (Exception e) {
                        logger.info("Failed to receive messages: " + e);
                    }
                }
            });
        } catch (Exception e) {
            logger.info("Failed to create receiver: " + e);
        }
        return client;
    }
}
