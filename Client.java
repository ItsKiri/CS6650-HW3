import org.apache.commons.imaging.ImageInfo;
import org.apache.commons.imaging.ImageReadException;
import org.apache.commons.imaging.Imaging;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.URLEncoder;


public class Client {

    private static final String CSV_FILE = "ApiStressTest.csv";
    private static BufferedWriter csvWriter;
    private static AtomicInteger successfulRequests = new AtomicInteger(0);
    private static AtomicInteger failedRequests = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException, IOException, ImageReadException {
        int threadGroupSize = Integer.parseInt(args[0]);
        int numThreadGroups = Integer.parseInt(args[1]);
        int delay = Integer.parseInt(args[2]) * 1000;
        String IPAddr = args[3];

        File imageFile = new File("/Users/kirimoto/Desktop/hw2/java-client-generated/src/main/java/nmtb.png");
        ImageInfo imageInfo = Imaging.getImageInfo(imageFile);

        String image = imageFile.length() + " " + imageInfo.getWidth() + "x" + imageInfo.getHeight();

        csvWriter = new BufferedWriter(new FileWriter(CSV_FILE));

        CountDownLatch initLatch = new CountDownLatch(2);
        for (int i = 0; i < 2; i++) {
            Thread thread = new Thread(() -> {
                for (int j = 0; j < 2; j++) {
                    sendRequest("http://" + IPAddr + "/AlbumStore/albums/", "POST", false, image);
                    sendRequest("http://" + IPAddr + "/AlbumStore/albums/review/test/like", "REVIEW", false, image);
                    sendRequest("http://" + IPAddr + "/AlbumStore/albums/review/test/like", "REVIEW", false, image);
                    sendRequest("http://" + IPAddr + "/AlbumStore/albums/review/test/dislike", "REVIEW", false, image);
                }
                initLatch.countDown();
            });
            thread.start();
        }

        initLatch.await();

        CountDownLatch groupLatch = new CountDownLatch(threadGroupSize * numThreadGroups);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numThreadGroups; i++) {
            for (int j = 0; j < threadGroupSize; j++) {
                Thread thread = new Thread(() -> {
                    for (int k = 0; k < 100; k++) {
                        sendRequest("http://" + IPAddr + "/AlbumStore/albums/", "POST", true, image);
                        sendRequest("http://" + IPAddr + "/AlbumStore/albums/review/test/like", "REVIEW", true, image);
                        sendRequest("http://" + IPAddr + "/AlbumStore/albums/review/test/like", "REVIEW", true, image);
                        sendRequest("http://" + IPAddr + "/AlbumStore/albums/review/test/dislike", "REVIEW", true, image);
                    }
                    groupLatch.countDown();
                });
                thread.start();
            }

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        groupLatch.await();

        long endTime = System.currentTimeMillis();
        double wallTime = (endTime - startTime) / 1000.0;
        double throughput = (successfulRequests.get() + failedRequests.get()) / wallTime;
        System.out.println("Wall Time: " + wallTime + " seconds");
        System.out.println("Throughput: " + throughput + " requests/second");
        System.out.println("Number of successful requests: " + successfulRequests.get());
        System.out.println("Number of failed requests: " + failedRequests.get());

        csvWriter.flush();
        csvWriter.close();

        computeAndPrintStatistics("POST");
        computeAndPrintStatistics("REVIEW");
    }

    private static void sendRequest(String targetURL, String method, Boolean flag, String image) {
        int retries = 5;
        HttpURLConnection httpURLConnection = null;
        while (retries > 0) {
            InputStream inputStream = null;
            try {
                URL url = new URL(targetURL);
                httpURLConnection = (HttpURLConnection) url.openConnection();
                long start = 0;
                if (flag) {
                    start = System.currentTimeMillis();
                }
                httpURLConnection.setRequestMethod("POST");
                if (method.equals("POST")) {
                    httpURLConnection.setDoOutput(true); // Allow Outputs
                    httpURLConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

                    String postData = "artist=" + URLEncoder.encode("test", "UTF-8") +
                            "&title=" + URLEncoder.encode("test", "UTF-8") +
                            "&year=" + URLEncoder.encode("test", "UTF-8") +
                            "&image=" + URLEncoder.encode(image, "UTF-8");

                    try (DataOutputStream wr = new DataOutputStream(httpURLConnection.getOutputStream())) {
                        wr.writeBytes(postData);
                        wr.flush();
                    }
                }

                int responseCode = httpURLConnection.getResponseCode();
                inputStream = httpURLConnection.getInputStream();

                if (flag) {
                    long end = System.currentTimeMillis();
                    long latency = end - start;
                    writeToCSV(start, method, latency, responseCode);
                }

                if (responseCode >= 200 && responseCode < 300) {
                    if (flag) {
                        successfulRequests.incrementAndGet();
                    }
                    return;
                } else if (responseCode >= 400 && responseCode < 600) {
                    if (flag) {
                        failedRequests.incrementAndGet();
                    }
                    retries--;
                }

            } catch (Exception e) {
                e.printStackTrace();
                retries--;
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (httpURLConnection != null) {
                    httpURLConnection.disconnect();
                }
            }
        }
    }


    private synchronized static void writeToCSV(long start, String method, long latency, int responseCode) throws IOException {
        csvWriter.write(start + "," + method + "," + latency + "," + responseCode);
        csvWriter.newLine();
    }

    private static void computeAndPrintStatistics(String method) throws IOException {
        List<Long> latencies = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(CSV_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts[1].equals(method)) {
                    latencies.add(Long.parseLong(parts[2]));
                }
            }
        }

        Collections.sort(latencies);
        double mean = latencies.stream().mapToLong(val -> val).average().orElse(0.0);
        long median = latencies.get(latencies.size() / 2);
        long p99 = latencies.get((int) (latencies.size() * 0.99));
        long min = latencies.get(0);
        long max = latencies.get(latencies.size() - 1);

        System.out.println("Statistics for " + method + ":");
        System.out.println("Mean response time: " + mean + "ms");
        System.out.println("Median response time: " + median + "ms");
        System.out.println("P99 response time: " + p99 + "ms");
        System.out.println("Min response time: " + min + "ms");
        System.out.println("Max response time: " + max + "ms");
    }
}
