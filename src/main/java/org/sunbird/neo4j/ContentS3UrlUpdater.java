package org.sunbird.neo4j;

import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.sunbird.util.Progress;
import org.sunbird.util.PropertiesCache;
import org.sunbird.util.logger.LoggerEnum;
import org.sunbird.util.logger.ProjectLogger;

import java.util.*;

public class ContentS3UrlUpdater {

    String defaultQuery = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content','ContentImage'] RETURN id(n) AS ID,n as NODE ORDER BY id(n) SKIP %s LIMIT %s;";
    String countQuery = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content','ContentImage'] RETURN count(*) as COUNT;";
    int contentSize;
    String oldS3Url;
    String newS3Url;
    String[] urlParmas = new String[]{"previewUrl", "downloadUrl", "artifactUrl", "posterImage", "appIcon", "streamingUrl", "toc_url"};
    List<Integer> failedIds = new ArrayList<>();

    public List<Integer> updateContentDataS3Urls() {
        boolean status = true;
        int skip = 0;
        int size = 500;
        try {
            Session session = ConnectionManager.getSession();
            if (session == null) {
                throw new Exception("Failed to get Session from the Neo4j Driver.");
            }

            Transaction transaction = session.beginTransaction();
            StatementResult result = transaction.run(countQuery);
            Futures.blockingGet(transaction.commitAsync(), () -> {});

            contentSize = result.next().get("COUNT").asInt();
            transaction.success();
            transaction.close();

            if (contentSize > 0) {
                long startTime = System.currentTimeMillis();
                getS3Urls();

                if(oldS3Url.isEmpty() || newS3Url.isEmpty()) {
                    ProjectLogger.log("Missing s3 urls.", LoggerEnum.INFO.name());
                } else {
                    while (status) {
                        transaction = session.beginTransaction();
                        String query = String.format(defaultQuery, skip, size);
                        result = transaction.run(query);

                        List<Record> records = result.list();
                        if (records.size() == 0) {
                            status = false;
                        } else if (records.size() < size) {
                            status = false;
                            updateS3Url(records, transaction);
                        } else {
                            updateS3Url(records, transaction);
                        }

                        Progress.printProgress(startTime, contentSize, (skip + records.size()));
                        Futures.blockingGet(transaction.commitAsync(), () -> {});
                        transaction.success();
                        transaction.close();
                        skip += size;
                    }
                }
            } else {
                ProjectLogger.log("No data of type Content or ContentImage in Neo4j.", LoggerEnum.INFO.name());
            }
            session.close();
        } catch (Exception e) {
            ProjectLogger.log("Failed to fetch data from Neo4j due to : " + e.getMessage(), e, LoggerEnum.ERROR.name());
            e.printStackTrace();
        }
        return failedIds;
    }


    private void updateS3Url(List<Record> records, Transaction transaction) {


        for(Record record : records) {
            int id = record.get("ID").asInt();
            Map<String, Object> node = record.get("NODE").asMap();
            try {
                String newValue;

                Map<String, Object> updateValues = new HashMap<>();

                for (String urlParam : urlParmas) {
                    if (node.containsKey(urlParam)) {
                        String value = node.get(urlParam).toString();
                        if(value.indexOf(oldS3Url) >= 0) {
                            newValue = node.get(urlParam).toString().replace(oldS3Url, newS3Url);
                            if(newValue.compareTo(value) != 0) {
                                updateValues.put(urlParam, newValue);
                            }
                        }
                    }
                }

                Object mimeTypeObject = node.get("mimeType");
                if (mimeTypeObject != null) {
                    String mimeType = mimeTypeObject.toString();
                    if (!mimeType.isEmpty() && mimeType.indexOf("image") >= 0) {
                        Object variantObject = node.get("variants");
                        if(variantObject != null) {
                            String variant = variantObject.toString();
                            String newVariant = variant.replaceAll(oldS3Url, newS3Url);
                            if(newVariant.compareTo(variant) != 0) {
                                updateValues.put("variants", newVariant);
                            }
                        }
                    }
                }

                if (updateValues.size() > 0) {
                    updateNode(id, updateValues, transaction);
                }
            } catch (Exception e) {
                ProjectLogger.log("Failed for Node Id : " + id + " due to " + e.getMessage(), e, LoggerEnum.ERROR.name());
                e.printStackTrace();
                failedIds.add(id);
            }
        }
    }

    private boolean updateNode(int id, Map<String, Object> updateValues, Transaction transaction) {
        try {
            StringBuilder updateQuery = new StringBuilder();
            if (updateValues.size() > 0) {
                updateQuery.append("MATCH (n) WHERE id(n)=").append(id).append(" SET ");
                for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
                    updateQuery.append("n.").append(entry.getKey());
                    updateQuery.append("='").append(entry.getValue().toString()).append("'");
                    updateQuery.append(",");
                }
                updateQuery = updateQuery.deleteCharAt(updateQuery.length()-1);
                updateQuery.append(" return n;");

//                ProjectLogger.log("Update query : " + updateQuery.toString());

                StatementResult result = transaction.run(updateQuery.toString());
                if (result.hasNext()) {
//                    ProjectLogger.log("Transaction Success");
                    return true;
                } else {
                    ProjectLogger.log("Transaction Failed", LoggerEnum.INFO.name());
                    failedIds.add(id);
                    return false;
                }
            }
        } catch (Exception e) {
            ProjectLogger.log("Failed for Node Id : " + id + " due to " + e.getMessage(), e, LoggerEnum.ERROR.name());
            e.printStackTrace();
            failedIds.add(id);
        }
        return false;
    }

    private void getS3Urls() {
        PropertiesCache propertiesCache = PropertiesCache.getInstance();
        oldS3Url = propertiesCache.getProperty("neo4j_old_s3url");
        newS3Url = propertiesCache.getProperty("neo4j_new_s3url");
    }

}
