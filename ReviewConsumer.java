package org.example;

import com.rabbitmq.client.*;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class ReviewConsumer {

    private final static String QUEUE_NAME = "reviews";
    private final static int THREAD_COUNT = 10;

    private final DynamoDbClient dynamoDbClient;
    private final ExecutorService executorService;

    public ReviewConsumer() {
        AwsSessionCredentials credentials = AwsSessionCredentials.create("ACCESS_KEY", "SECRET_KEY", "SESSION_TOKEN");
        dynamoDbClient = DynamoDbClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(Region.US_WEST_2)
                .build();

        executorService = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    public void start() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("EC2_Host");
        factory.setPort(5672);

        Connection connection = factory.newConnection();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.submit(() -> {
                try {
                    Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), "UTF-8");
                        processMessage(message);
                    };
                    channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private void processMessage(String message) {
        String[] parts = message.split(":");
        if (parts.length < 2) return;

        String albumID = parts[0];
        String review = parts[1];

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("albumID", AttributeValue.builder().s(albumID).build());

        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName("Albums")
                .key(key)
                .build();

        GetItemResponse getItemResponse = dynamoDbClient.getItem(getItemRequest);
        Map<String, AttributeValue> item = getItemResponse.item();
        if (item == null || item.isEmpty()) return;

        int count = Integer.parseInt(item.get(review).n()) + 1;

        Map<String, AttributeValueUpdate> updates = new HashMap<>();
        updates.put(review, AttributeValueUpdate.builder()
                .value(AttributeValue.builder().n(String.valueOf(count)).build())
                .action(AttributeAction.PUT)
                .build());

        UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                .tableName("Albums")
                .key(key)
                .attributeUpdates(updates)
                .build();

        dynamoDbClient.updateItem(updateItemRequest);
    }


    public static void main(String[] argv) throws IOException, TimeoutException {
        new ReviewConsumer().start();
    }
}
