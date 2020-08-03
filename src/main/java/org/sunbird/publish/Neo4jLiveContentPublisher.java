package org.sunbird.publish;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.neo4j.driver.v1.Session;
import org.sunbird.neo4j.SearchOperation;
import org.sunbird.util.Progress;
import org.sunbird.util.PropertiesCache;
import org.sunbird.util.logger.LoggerEnum;
import org.sunbird.util.logger.ProjectLogger;

import java.io.BufferedWriter;
import java.io.FileWriter;
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
                ProjectLogger.log("Count of data to Publish : " + contentSize, LoggerEnum.INFO.name());

                if (contentSize > 0) {
                    long startTime = System.currentTimeMillis();
                    while (status) {
                        List<Object> contentDataForAssets = searchOperation.getAllLiveContentIds(skip, size);
                        ProjectLogger.log(contentDataForAssets.toString(), LoggerEnum.INFO.name());
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
                    ProjectLogger.log("No Live data of Content in Neo4j.", LoggerEnum.INFO.name());
                }

                if (failStatus) {
                    ProjectLogger.log("", LoggerEnum.INFO.name());
                    ProjectLogger.log("Published Failed for some content ids.", LoggerEnum.INFO.name());
                    ProjectLogger.log("Please check the Error File", LoggerEnum.INFO.name());
                } else {
                    ProjectLogger.log("Published Successfully for all Live Content of Neo4j.", LoggerEnum.INFO.name());
                }
            } catch (Exception e) {
                ProjectLogger.log(e.getMessage(), e, LoggerEnum.ERROR.name());
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
            ProjectLogger.log("", LoggerEnum.INFO.name());
            ProjectLogger.log("Published Failed for some content ids.", LoggerEnum.INFO.name());
            ProjectLogger.log("Please check the Error File", LoggerEnum.INFO.name());
        } else {
            ProjectLogger.log("Published Successfully for all Live Content of Neo4j.", LoggerEnum.INFO.name());
        }
    } catch (Exception e) {
        ProjectLogger.log(e.getMessage(), e, LoggerEnum.ERROR.name());
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
            ProjectLogger.log("An error occurred while shutting down the Executor Service : " + ex.getMessage(), ex, LoggerEnum.ERROR.name());
            ex.printStackTrace();
        }
    }

        public void verifyProperties() throws Exception {

            PropertiesCache propertiesCache = PropertiesCache.getInstance();

            String hostUrl = propertiesCache.getProperty("host_url");
            String publishUrl = propertiesCache.getProperty("publish_api_url");
            url = hostUrl + publishUrl;
            publisherId = propertiesCache.getProperty("publisher_user_id");
            publisherName = propertiesCache.getProperty("publisher_name");
            authorization = propertiesCache.getProperty("authorization");
            authToken = propertiesCache.getProperty("auth_token");

            if (hostUrl == null || hostUrl.isEmpty()) {
                ProjectLogger.log("Missing Host URL", LoggerEnum.INFO.name());
                ProjectLogger.log("Failed to publish content", LoggerEnum.INFO.name());
                throw new Exception("Missing Host Url.");
            }
            if (publishUrl == null || publishUrl.isEmpty()) {
                ProjectLogger.log("Missing Publish Api URL", LoggerEnum.INFO.name());
                ProjectLogger.log("Failed to publish content", LoggerEnum.INFO.name());
                throw new Exception("Missing Publish Api Url.");
            }
            if (publisherId == null || publisherId.isEmpty()) {
                ProjectLogger.log("Missing Publisher User ID.", LoggerEnum.INFO.name());
                ProjectLogger.log("Failed to publish content", LoggerEnum.INFO.name());
                throw new Exception("Missing Publisher User ID.");
            }
            if (publisherName == null || publisherName.isEmpty()) {
                ProjectLogger.log("Missing Publisher Name", LoggerEnum.INFO.name());
                ProjectLogger.log("Failed to publish content", LoggerEnum.INFO.name());
                throw new Exception("Missing Publisher Name.");
            }
            if (authorization == null || authorization.isEmpty()) {
                ProjectLogger.log("Missing Authorization Key", LoggerEnum.INFO.name());
                ProjectLogger.log("Failed to publish content", LoggerEnum.INFO.name());
                throw new Exception("Missing Authorization Key.");
            }
            if (authToken == null || authToken.isEmpty()) {
                ProjectLogger.log("Missing Auth Token", LoggerEnum.INFO.name());
                ProjectLogger.log("Failed to publish content", LoggerEnum.INFO.name());
                throw new Exception("Missing Auth Token.");
            }
        }


    private List<String> publishContent(List<Object> publishContentIds, ExecutorService executor) throws Exception {

        List<String> failureIds = new ArrayList<>();

        try {

            for(int i = 0; i < publishContentIds.size(); i++) {
                String contentId = ((String) publishContentIds.get(i)).trim();
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

//                ProjectLogger.log("JSON : " + json);
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
                ProjectLogger.log("Exception occurred while waiting for the result : " + e.getMessage(), e, LoggerEnum.ERROR.name());
                e.printStackTrace();
            }

        }catch (Exception e) {
            ProjectLogger.log("Failed due to Unknown Exception : " + e.getMessage(), e, LoggerEnum.ERROR.name());
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
//                ProjectLogger.log("Command : " + commandToRun);
//                ProjectLogger.log("Publishing content with Id : " + id);
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
                ProjectLogger.log("apiStatus for Content id "+ id + " is : " + apiStatus, LoggerEnum.INFO.name());
                if (apiStatus == 200) {
                    return "true";
                } else {
                    return id;
                }
//                return true;


            } catch (Exception e) {
                ProjectLogger.log("Some error occurred while publishing the content : " + id, e, LoggerEnum.ERROR.name());
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
            ProjectLogger.log("Failed to Write the File", e, LoggerEnum.ERROR.name());
            e.printStackTrace();
        }
    }
}
