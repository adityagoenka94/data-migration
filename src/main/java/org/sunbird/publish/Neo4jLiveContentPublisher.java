package org.sunbird.publish;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.sunbird.neo4j.SearchOperation;
import org.sunbird.util.PropertiesCache;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Neo4jLiveContentPublisher {

    public boolean publishAllContents() {

        int success = 0;
        List<String> failureIds = new ArrayList<>();
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            PropertiesCache propertiesCache = PropertiesCache.getInstance();

            String hostUrl = propertiesCache.getProperty("host_url");
            String publishUrl = propertiesCache.getProperty("publish_api_url");
            String url = hostUrl + publishUrl;
            String publisherId = propertiesCache.getProperty("publisher_user_id");
            String publisherName = propertiesCache.getProperty("publisher_name");

            if(hostUrl == null || hostUrl.isEmpty()) {
                System.out.println("Missing Host URL");
                System.out.println("Failed to publish content");
                return false;
            }
            if(publishUrl == null || publishUrl.isEmpty()) {
                System.out.println("Missing Publish Api URL");
                System.out.println("Failed to publish content");
                return false;
            }
            if(publisherId == null || publisherId.isEmpty()) {
                System.out.println("Missing Publisher User ID.");
                System.out.println("Failed to publish content");
                return false;
            }
            if(publisherName == null || publisherName.isEmpty()) {
                System.out.println("Missing Publisher Name");
                System.out.println("Failed to publish content");
                return false;
            }

            SearchOperation searchOperation = new SearchOperation();
            List conentIds = searchOperation.getAllLiveContentIds();
            int count = conentIds.size();
            long startTime = System.currentTimeMillis();
            System.out.println("Count of data to Publish : " + count);

            for (int i = 0; i < count; i++) {
                String contentId = (String) conentIds.get(i);
                HttpPost httpPost = new HttpPost(url + contentId);
                String json = "{\"request\": {\"content\": { \"publisher\": \""+ publisherName +"\", \"lastPublishedBy\": \""+ publisherId +"\" } } }";
                StringEntity entity = new StringEntity(json);
                httpPost.setEntity(entity);
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-type", "application/json");

//                System.out.println("JSON : " + json);
//                return true;
                CloseableHttpResponse response = client.execute(httpPost);
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    success++;
                } else {
                    failureIds.add(contentId);
                }
                printProgress(startTime, count, i+1);
            }

            if(failureIds.size() > 0) {
                System.out.println("Only " + success + " Content Successfully Published.");
                System.out.println("Failed to Publish " + failureIds.size() + " Contents.");
                System.out.println("Content Ids for failed : " + failureIds);
            } else {
                System.out.println("Successfully Publish " + success + " Content.");
            }

        } catch (UnsupportedEncodingException e) {
            System.out.println("Failed due to Unsupported Entity Exception : " + e.getMessage());
            e.printStackTrace();
            return false;
        } catch (ClientProtocolException e) {
            System.out.println("Failed due to Client Protocol Exception : " + e.getMessage());
            e.printStackTrace();
            return false;
        } catch (Exception e) {
            System.out.println("Failed due to Unknown Exception : " + e.getMessage());
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static void printProgress(long startTime, long total, long current) {
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
