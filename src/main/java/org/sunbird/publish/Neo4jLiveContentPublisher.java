package org.sunbird.publish;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.neo4j.driver.v1.Session;
import org.sunbird.neo4j.ConnectionManager;
import org.sunbird.neo4j.SearchOperation;
import org.sunbird.util.Progress;
import org.sunbird.util.PropertiesCache;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class Neo4jLiveContentPublisher {

    String url;
    String publisherId;
    String publisherName;
    String authorization;
    String authToken;
    List<Future<String>> status = new ArrayList<>();


    public void publishAllContents() {

            SearchOperation searchOperation = new SearchOperation();

            boolean status = true;
            boolean failStatus = false;
            int skip = 0;
            int size = 100;
            String fileName = "Error_Publish_" + System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(5);
        Session session = null;
            try {

                verifyProperties();

                List<String> contentFailed;
                int contentSize = searchOperation.getAllLiveContentCount();
                System.out.println("Count of data to Publish : " + contentSize);

                if (contentSize > 0) {
                    long startTime = System.currentTimeMillis();
                    while (status) {
                        List<Object> contentDataForAssets = searchOperation.getAllLiveContentIds(skip, size);
                        System.out.println(contentDataForAssets);
                        contentFailed = publishContent(contentDataForAssets, executor);
                        if (contentFailed.size() > 0) {
                            appendToFile(contentFailed, fileName);
                            failStatus = true;
                        }

                        Progress.printProgress(startTime, contentSize, (skip + contentDataForAssets.size()));

                        skip += size;

                        if (skip >= contentSize) {
                            status = false;
                        }
                    }
                } else {
                    System.out.println("No Live data of Content in Neo4j.");
                }

                if (failStatus) {
                    System.out.println();
                    System.out.println("Published Failed for some content ids.");
                    System.out.println("Please check the Error File");
                } else {
                    System.out.println("Published Successfully for all Live Content of Neo4j.");
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        this.awaitTerminationAfterShutdown(executor);
    }

    public void publishContentsForIds(String[] contentIds) {
        boolean failStatus = false;
        List<String> contentFailed;
        ExecutorService executor = Executors.newFixedThreadPool(1);
        String fileName = "Error_Publish_" + System.currentTimeMillis();
        try {
        List<Object> contentData = Arrays.asList(contentIds);
        contentFailed = publishContent(contentData, executor);
        if (contentFailed.size() > 0) {
            appendToFile(contentFailed, fileName);
            failStatus = true;
        }
        if (failStatus) {
            System.out.println();
            System.out.println("Published Failed for some content ids.");
            System.out.println("Please check the Error File");
        } else {
            System.out.println("Published Successfully for all Live Content of Neo4j.");
        }
    } catch (Exception e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
    }
        this.awaitTerminationAfterShutdown(executor);
    }

    private void awaitTerminationAfterShutdown(ExecutorService threadPool) {

        try {
            threadPool.shutdown();
        } catch (Exception ex) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
            System.out.println("An error occurred while shutting down the Executor Service : " + ex.getMessage());
            ex.printStackTrace();
        }
    }

        private void verifyProperties() throws Exception {

            PropertiesCache propertiesCache = PropertiesCache.getInstance();

            String hostUrl = propertiesCache.getProperty("host_url");
            String publishUrl = propertiesCache.getProperty("publish_api_url");
            url = hostUrl + publishUrl;
            publisherId = propertiesCache.getProperty("publisher_user_id");
            publisherName = propertiesCache.getProperty("publisher_name");
            authorization = propertiesCache.getProperty("authorization");
            authToken = propertiesCache.getProperty("auth_token");

            if (hostUrl == null || hostUrl.isEmpty()) {
                System.out.println("Missing Host URL");
                System.out.println("Failed to publish content");
                throw new Exception("Missing Host Url.");
            }
            if (publishUrl == null || publishUrl.isEmpty()) {
                System.out.println("Missing Publish Api URL");
                System.out.println("Failed to publish content");
                throw new Exception("Missing Publish Api Url.");
            }
            if (publisherId == null || publisherId.isEmpty()) {
                System.out.println("Missing Publisher User ID.");
                System.out.println("Failed to publish content");
                throw new Exception("Missing Publisher User ID.");
            }
            if (publisherName == null || publisherName.isEmpty()) {
                System.out.println("Missing Publisher Name");
                System.out.println("Failed to publish content");
                throw new Exception("Missing Publisher Name.");
            }
            if (authorization == null || authorization.isEmpty()) {
                System.out.println("Missing Authorization Key");
                System.out.println("Failed to publish content");
                throw new Exception("Missing Authorization Key.");
            }
            if (authToken == null || authToken.isEmpty()) {
                System.out.println("Missing Auth Token");
                System.out.println("Failed to publish content");
                throw new Exception("Missing Auth Token.");
            }
        }


    private List<String> publishContent(List<Object> publishContentIds, ExecutorService executor) throws Exception {

        List<String> failureIds = new ArrayList<>();

        try {

            for(int i = 0; i < publishContentIds.size(); i++) {
                String contentId = (String) publishContentIds.get(i);
//                HttpPost httpPost = new HttpPost(url + contentId);
//                String json = "{\"request\": {\"content\": { \"publisher\": \""+ publisherName +"\", \"lastPublishedBy\": \""+ publisherId +"\" } } }";
//                StringEntity entity = new StringEntity(json);
//                httpPost.setEntity(entity);
//                httpPost.setHeader("Accept", "application/json");
//                httpPost.setHeader("Content-type", "application/json");
//                if(authorization.startsWith("Bearer")) {
//                    httpPost.setHeader("Authorization", authorization);
//                } else {
//                    httpPost.setHeader("Authorization", "Bearer "+authorization);
//                }
//                httpPost.setHeader("x-authenticated-user-token", authToken);

//                System.out.println("JSON : " + json);
//                return true;
                status.add(executor.submit(new CallableThread(contentId)));

//                CloseableHttpResponse response = client.execute(httpPost);
//                int apiStatus = response.getStatusLine().getStatusCode();
//                if (apiStatus != 200) {
//                    failureIds.add(contentId);
//                }
            }
            try {
                int statusSize = status.size();
                for(int i=0; i < statusSize; i++) {
                    String response = status.get(i).get();
                    if(!response.equals("true")) {
                        failureIds.add(response);
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("Exception occurred while waiting for the result : " + e.getMessage());
                e.printStackTrace();
            }

        }catch (Exception e) {
            System.out.println("Failed due to Unknown Exception : " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
        return failureIds;
    }

    class CallableThread implements Callable<String> {

        private String id;

        public CallableThread(String id) {
            this.id = id;
        }


        @Override
        public String call() {
            try (CloseableHttpClient client = HttpClients.createDefault()) {
//                System.out.println("Command : " + commandToRun);
//                System.out.println("Publishing content with Id : " + id);
                HttpPost httpPost = new HttpPost(url + id);
                String json = "{\"request\": {\"content\": { \"publisher\": \""+ publisherName +"\", \"lastPublishedBy\": \""+ publisherId +"\" } } }";
                StringEntity entity = new StringEntity(json);
                httpPost.setEntity(entity);
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-type", "application/json");
                if(authorization.startsWith("Bearer")) {
                    httpPost.setHeader("Authorization", authorization);
                } else {
                    httpPost.setHeader("Authorization", "Bearer "+authorization);
                }
                httpPost.setHeader("x-authenticated-user-token", authToken);
                CloseableHttpResponse response = client.execute(httpPost);
                int apiStatus = response.getStatusLine().getStatusCode();
                System.out.println("apiStatus for Content id "+ id + " is : " + apiStatus);
                if (apiStatus == 200) {
                    return "true";
                } else {
                    return id;
                }
//                return true;


            } catch (Exception e) {
                System.out.println("Some error occurred while publishing the content : " + id);
                e.printStackTrace();
                return id;
            }
        }
    }



    public static void appendToFile(List<String> data, String fileName) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            writer.append(data.toString());

            writer.close();
        } catch (Exception e) {
            System.out.println("Failed to Write the File");
            e.printStackTrace();
        }
    }
}
