package org.sunbird.cassandra;

import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class SearchOperation {

    public List<String> getAllFrameworkIdentifier() {

        try {
            Session session = ConnectionManager.getSession();
            String query = "SELECT identifier from hierarchy_store.framework_hierarchy;";
            ResultSet rs = session.execute(query);

            List<String> frameworks = new ArrayList<>();

            for(Row row : rs) {
                frameworks.add(row.getString("identifier"));
            }
            return frameworks;
        } catch (Exception e) {
            System.out.println("Failed to get Framework List : " + e.getMessage());
            return null;
        }
    }

    public List<String> getAllContentHierarchyIdentifier() {

        try {
            System.out.println("Fetching content ids from content hierarchy");
            int count = 0;
            int batchSize = 500;
            Session session = ConnectionManager.getSession();
            String query = "SELECT identifier from hierarchy_store.content_hierarchy;";
//            ResultSet rs = session.execute(query);

            List<String> contentHierarchy = new ArrayList<>();

            Statement stmt = new SimpleStatement(query);
            stmt.setFetchSize(batchSize);
            ResultSet rs = session.execute(stmt);

            for(Row row : rs) {
                count++;
                contentHierarchy.add(row.getString("identifier"));
                if (count % batchSize == 0) {
                    System.out.println("Fetching More Data...");
                }
            }
            System.out.println("Completed");
            System.out.println("Size of the content hierarchy ; " + contentHierarchy.size());
            return contentHierarchy;
        } catch (Exception e) {
            System.out.println("Failed to get Content Hierarchy : " + e.getMessage());
            return null;
        }
    }

    public List<String> getAllContentDataIdentifier() {

        try {
            System.out.println("Fetching content ids from content data");
            int count = 0;
            int batchSize = 5;
            Session session = ConnectionManager.getSession();
            List<String> contentData = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            String query = "SELECT content_id from content_store.content_data;";
            Statement stmt = new SimpleStatement(query);
            stmt.setFetchSize(batchSize);
            ResultSet rs = session.execute(stmt);
//            Iterator<Row> iter = rs.iterator();
//            boolean status = true;
//            while(status) {
//                count++;
//                for(Row row : rs) {
//                    contentData.add(row.getString("content_id"));
//                }
//                System.out.println("Count ; " + count);
//                if(!rs.isFullyFetched()) {
//                    rs.fetchMoreResults();
//                } else {
//                    status =false;
//                }
//            }

            for(Row row : rs) {
                count++;
                contentData.add(row.getString("content_id"));
                if (count % batchSize == 0) {
                    System.out.println("Fetching More Data...");
                }
            }
            System.out.println("Completed");
            System.out.println("Size of the content data ; " + contentData.size());
//            System.out.println("Count ; " + count);

            return contentData;
        } catch (Exception e) {
            System.out.println("Failed to get Content Data : " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

}
