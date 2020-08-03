package org.sunbird.cassandra;

import com.datastax.driver.core.*;
import org.sunbird.util.logger.LoggerEnum;
import org.sunbird.util.logger.ProjectLogger;

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
            ProjectLogger.log("Failed to get Framework List : " + e.getMessage(), e, LoggerEnum.ERROR.name());
            return null;
        }
    }

    public List<String> getAllContentHierarchyIdentifier() {

        try {
            ProjectLogger.log("Fetching content ids from content hierarchy", LoggerEnum.INFO.name());
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
                    ProjectLogger.log("Fetching More Data...", LoggerEnum.INFO.name());
                }
            }
            ProjectLogger.log("Completed", LoggerEnum.INFO.name());
            ProjectLogger.log("Size of the content hierarchy ; " + contentHierarchy.size(), LoggerEnum.INFO.name());
            return contentHierarchy;
        } catch (Exception e) {
            ProjectLogger.log("Failed to get Content Hierarchy : " + e.getMessage(), e, LoggerEnum.ERROR.name());
            return null;
        }
    }

    public List<String> getAllContentDataIdentifier() {

        try {
            ProjectLogger.log("Fetching content ids from content data", LoggerEnum.INFO.name());
            int count = 0;
            int batchSize = 500;
            Session session = ConnectionManager.getSession();
            List<String> contentData = new ArrayList<>();
            long startTime = System.currentTimeMillis();
            String query = "SELECT content_id from content_store.content_data;";
            Statement stmt = new SimpleStatement(query);
            stmt.setFetchSize(batchSize);
            ResultSet rs = session.execute(stmt);

            for(Row row : rs) {
                count++;
                contentData.add(row.getString("content_id"));
                if (count % batchSize == 0) {
                    ProjectLogger.log("Fetching More Data...", LoggerEnum.INFO.name());
                }
            }
            ProjectLogger.log("Completed", LoggerEnum.INFO.name());
            ProjectLogger.log("Size of the content data ; " + contentData.size(), LoggerEnum.INFO.name());
//            ProjectLogger.log("Count ; " + count);

            return contentData;
        } catch (Exception e) {
            ProjectLogger.log("Failed to get Content Data : " + e.getMessage(), e, LoggerEnum.ERROR.name());
            e.printStackTrace();
            return null;
        }
    }

}
