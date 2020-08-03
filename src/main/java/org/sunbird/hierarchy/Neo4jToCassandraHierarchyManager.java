package org.sunbird.hierarchy;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
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

public class Neo4jToCassandraHierarchyManager {

    String url;
    String authorization;
    List<Future<String>> status = new ArrayList<>();


    public void migrateAllContentsHierarchy() {

        SearchOperation searchOperation = new SearchOperation();

        boolean status = true;
        boolean failStatus = false;
        int skip = 0;
        int size = 100;
        String fileName = "Error_Hierarchy_" + System.currentTimeMillis();
        ExecutorService executor = Executors.newFixedThreadPool(5);
        Session session = null;
        try {

            verifyProperties();

            List<String> contentFailed;
            int contentSize = searchOperation.getAllCollectionContentCount();
            ProjectLogger.log("Count of data to migrate : " + contentSize, LoggerEnum.INFO.name());

            if (contentSize > 0) {
                long startTime = System.currentTimeMillis();
                while (status) {
                    List<Object> contentDataForAssets = searchOperation.getAllCollectionContentIds(skip, size);
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
                ProjectLogger.log("No Collection type data in Neo4j.", LoggerEnum.INFO.name());
            }

            if (failStatus) {
                ProjectLogger.log("", LoggerEnum.INFO.name());
                ProjectLogger.log("Migration Failed for some content ids.", LoggerEnum.INFO.name());
                ProjectLogger.log("Please check the Error File", LoggerEnum.INFO.name());
            } else {
                ProjectLogger.log("Migrated Successfully for all Collection Contents of Neo4j.", LoggerEnum.INFO.name());
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
        String fileName = "Error_Hierarchy_" + System.currentTimeMillis();
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
        String hierarchyUrl = propertiesCache.getProperty("hierarchy_api_url");
        url = hostUrl + hierarchyUrl;
        authorization = propertiesCache.getProperty("authorization");

        if (hostUrl == null || hostUrl.isEmpty()) {
            ProjectLogger.log("Missing Host URL", LoggerEnum.INFO.name());
            ProjectLogger.log("Failed to Migrate content hierarchy", LoggerEnum.INFO.name());
            throw new Exception("Missing Host Url.");
        }
        if (hierarchyUrl == null || hierarchyUrl.isEmpty()) {
            ProjectLogger.log("Missing Hierarchy Api URL", LoggerEnum.INFO.name());
            ProjectLogger.log("Failed to migrate content hierarchy", LoggerEnum.INFO.name());
            throw new Exception("Missing Hierarchy Api Url.");
        }
        if (authorization == null || authorization.isEmpty()) {
            ProjectLogger.log("Missing Authorization Key", LoggerEnum.INFO.name());
            ProjectLogger.log("Failed to publish content", LoggerEnum.INFO.name());
            throw new Exception("Missing Authorization Key.");
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
                status.add(executor.submit(new org.sunbird.hierarchy.Neo4jToCassandraHierarchyManager.CallableThread(contentId)));

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
                HttpGet httpGet = new HttpGet(url + id + "?mode=edit");
                if(authorization.startsWith("Bearer")) {
                    httpGet.setHeader("Authorization", authorization);
                } else {
                    httpGet.setHeader("Authorization", "Bearer "+authorization);
                }
                CloseableHttpResponse response = client.execute(httpGet);
                Thread.sleep(100);
                int apiStatus = response.getStatusLine().getStatusCode();
                ProjectLogger.log("apiStatus for Content id "+ id + " is : " + apiStatus, LoggerEnum.INFO.name());
                if (apiStatus == 200) {
                    return "true";
                } else {
                    return id;
                }
//                return true;


            } catch (Exception e) {
                ProjectLogger.log("Some error occurred while Migrating the content Hierarchy for : " + id, e, LoggerEnum.ERROR.name());
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
