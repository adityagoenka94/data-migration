package org.sunbird.neo4j;

import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.sunbird.util.PropertiesCache;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class ContentS3UrlUpdater {

    String defaultQuery = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content','ContentImage'] RETURN id(n) AS ID,n as NODE ORDER BY id(n) SKIP %s LIMIT %s;";
    String countQuery = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content','ContentImage'] RETURN count(*) as COUNT;";
    int contentSize;
    String oldS3Url;
    String newS3Url;
    String[] urlParmas = new String[]{"previewUrl", "downloadUrl", "artifactUrl", "posterImage", "appIcon", "streamingUrl", "toc_url"};
    List<Integer> failedIds = new ArrayList<>();

    public List<Integer> updateContentDataS3Urls() {
        Map<String, String> contentData = new HashMap<>();
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
                    System.out.println("Missing s3 urls.");
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

                        printProgress(startTime, contentSize, (skip + records.size()));
                        Futures.blockingGet(transaction.commitAsync(), () -> {});
                        transaction.success();
                        transaction.close();
                        skip += size;
                    }
                }
            } else {
                System.out.println("No data of type Content or ContentImage in Neo4j.");
            }
//            transaction.close();
            session.close();
        } catch (Exception e) {
            System.out.println("Failed to fetch data from Neo4j due to : " + e.getMessage());
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
                System.out.println("Failed for Node Id : " + id + " due to " + e.getMessage());
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

//                System.out.println("Update query : " + updateQuery.toString());

                StatementResult result = transaction.run(updateQuery.toString());
                if (result.hasNext()) {
//                    System.out.println("Transaction Success");
                    return true;
                } else {
                    System.out.println("Transaction Failed");
                    failedIds.add(id);
                    return false;
                }
            }
        } catch (Exception e) {
            System.out.println("Failed for Node Id : " + id + " due to " + e.getMessage());
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


    private void printProgress(long startTime, long total, long current) {
        long eta = current == 0 ? 0 :
                (total - current) * (System.currentTimeMillis() - startTime) / current;

        String etaHms = current == 0 ? "N/A" :
                String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(eta),
                        TimeUnit.MILLISECONDS.toMinutes(eta) % TimeUnit.HOURS.toMinutes(1),
                        TimeUnit.MILLISECONDS.toSeconds(eta) % TimeUnit.MINUTES.toSeconds(1));

        StringBuilder string = new StringBuilder(140);
        int percent = (int) (current * 100 / total);
        string
                .append('\r')
                .append(String.join("", Collections.nCopies(percent == 0 ? 2 : 2 - (int) (Math.log10(percent)), " ")))
                .append(String.format(" %d%% [", percent))
                .append(String.join("", Collections.nCopies(percent, "=")))
                .append('>')
                .append(String.join("", Collections.nCopies(100 - percent, " ")))
                .append(']')
                .append(String.join("", Collections.nCopies((int) (Math.log10(total)) - (int) (Math.log10(current)), " ")))
                .append(String.format(" %d/%d, ETA: %s", current, total, etaHms));

        System.out.print(string);
    }
}
