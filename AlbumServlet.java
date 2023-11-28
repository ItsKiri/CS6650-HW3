package com.servlet;

import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@WebServlet(name = "AlbumServlet", urlPatterns = {"/AlbumStore/albums/*"})
public class AlbumServlet extends HttpServlet {

    private final String TABLE_NAME = "Albums";
    private final static String QUEUE_NAME = "reviews";
    private final static int CHANNEL_POOL_SIZE = 10;

    private DynamoDbClient dynamoDbClient;
    private BlockingQueue<Channel> channelPool;

    @Override
    public void init() throws ServletException {
        super.init();

        AwsSessionCredentials credentials = AwsSessionCredentials.create("ACCESS_KEY", "SECRET_KEY", "SESSION_TOKEN");
        dynamoDbClient = DynamoDbClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(Region.US_WEST_2)
                .build();

        initializeChannelPool();
    }



    private void initializeChannelPool() throws ServletException {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setPort(5672);
            Connection connection = factory.newConnection();

            channelPool = new ArrayBlockingQueue<>(CHANNEL_POOL_SIZE);

            for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
                Channel channel = connection.createChannel();
                channelPool.put(channel);
            }
        } catch (Exception e) {
            throw new ServletException("Failed to initialize channel pool", e);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String pathInfo = req.getPathInfo();

        if (pathInfo.contains("/review/")) {
            doPostReview(req, resp);
        } else {
            doPostAlbum(req, resp);
        }
    }

    private void doPostAlbum(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String artist = req.getParameter("artist");
        String title = req.getParameter("title");
        String year = req.getParameter("year");
        String image = req.getParameter("image");
        String likeornot = "0";

        String albumID = UUID.randomUUID().toString();

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("albumID", AttributeValue.builder().s(albumID).build());
        item.put("artist", AttributeValue.builder().s(artist).build());
        item.put("title", AttributeValue.builder().s(title).build());
        item.put("year", AttributeValue.builder().s(year).build());
        item.put("image", AttributeValue.builder().s(image).build());
        item.put("likeornot", AttributeValue.builder().n(likeornot).build());

        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(TABLE_NAME)
                .item(item)
                .build();

        dynamoDbClient.putItem(putItemRequest);

        JsonObject responseJson = new JsonObject();
        responseJson.addProperty("albumID", albumID);

        resp.setContentType("application/json");
        resp.getWriter().write(responseJson.toString());
    }

    private void doPostReview(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String[] paths = req.getPathInfo().split("/");

        if (paths.length < 4) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        String albumID = paths[paths.length - 2];
        String likeOrNot = paths[paths.length - 1];

        if (!likeOrNot.equals("like") && !likeOrNot.equals("dislike")) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        sendReviewToQueue(albumID, likeOrNot);
        resp.setStatus(HttpServletResponse.SC_ACCEPTED);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String[] paths = req.getPathInfo().split("/");
        String albumID = paths[paths.length - 1];

        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(TABLE_NAME)
                .key(Map.of("albumID", AttributeValue.builder().s(albumID).build()))
                .build();

        GetItemResponse getItemResponse = dynamoDbClient.getItem(getItemRequest);

        Map<String, AttributeValue> retrievedItem = getItemResponse.item();

        if (retrievedItem == null || retrievedItem.isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        JsonObject albumInfo = new JsonObject();
        albumInfo.addProperty("artist", retrievedItem.get("artist").s());
        albumInfo.addProperty("title", retrievedItem.get("title").s());
        albumInfo.addProperty("year", retrievedItem.get("year").s());
        albumInfo.addProperty("image", retrievedItem.get("image").s());

        resp.setContentType("application/json");
        resp.getWriter().write(albumInfo.toString());
    }

    private void sendReviewToQueue(String albumID, String review) {
        try {
            Channel channel = channelPool.take();

            String message = albumID + ":" + review;
            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");

            channelPool.put(channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
