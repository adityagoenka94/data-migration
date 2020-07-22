package org.sunbird.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.sunbird.util.Progress;

import java.util.List;

public class DeleteOperation {

    SearchOperation searchOperation = new SearchOperation();
    org.sunbird.neo4j.SearchOperation neo4jSearch = new org.sunbird.neo4j.SearchOperation();
    String[] retainFrameworks = {"NCF"};

    public void deleteFrameworkHierarchy() {

        try {
            Session session = ConnectionManager.getSession();
            List<String> framework = searchOperation.getAllFrameworkIdentifier();
            for(String frameId : retainFrameworks) {
                if (framework.contains(frameId)) {
                    framework.remove(frameId);
                }
            }


            if(framework !=null && framework.size() > 0) {
                StringBuilder query = new StringBuilder("DELETE FROM hierarchy_store.framework_hierarchy WHERE identifier IN (");
                for (String frame : framework) {
                    query.append("'");
                    query.append(frame);
                    query.append("',");
                }
                query = query.deleteCharAt(query.lastIndexOf(","));
                query.append(");");

                System.out.println("Query to delete Framework hierarchy data : "+query);

                ResultSet rs = session.execute(query.toString());
            }
            System.out.println("Successfully delete Framework hierarchy other than 'NCF'.");
        } catch (Exception e) {
            System.out.println("Failed to delete Framework hierarchy : " + e.getMessage());
            e.printStackTrace();
        }
    }


    public void deleteContentHierarchy() {

        try {
            Session session = ConnectionManager.getSession();
            List<String> contentIdentifier = searchOperation.getAllContentHierarchyIdentifier();
            List neo4jContentList = neo4jSearch.getContentIds();

            for(Object identity : neo4jContentList) {
                String identifier = (String)identity;
                if(contentIdentifier.contains(identifier)) {
                    contentIdentifier.remove(identifier);
                }
            }

            neo4jContentList = null;

            int total = contentIdentifier.size();
            System.out.println("Deleting content hierarchy data for count = "+ total);

            int current = 0;
            long startTime = System.currentTimeMillis();
            while(current < total) {
                int batch;
                if (current + 50 <= total) {
                    batch = 50;
                } else {
                    batch = total - current;
                }

                StringBuilder query = new StringBuilder("DELETE FROM hierarchy_store.content_hierarchy WHERE identifier IN (");
                for(int i = 0; i < batch; i++) {
                        query.append("'");
                        query.append(contentIdentifier.get(current + i));
                        query.append("',");

                }
                query = query.deleteCharAt(query.lastIndexOf(","));
                query.append(");");

//                System.out.println("Query to delete content hierarchy data : "+query);
                ResultSet rs = session.execute(query.toString());
                current += batch;
                Progress.printProgress(startTime, total, current);
            }

//            printProgress(startTime, total, current);
            System.out.println("Successfully deleted hierarchy for contents not present in Neo4j.");
        } catch (Exception e) {
            System.out.println("Failed to delete content hierarchy : " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void deleteContentData() {

        try {
            Session session = ConnectionManager.getSession();
            List<String> contentIdentifier = searchOperation.getAllContentDataIdentifier();
            if(contentIdentifier != null) {
                System.out.println("content data for count = " + contentIdentifier.size());
                List neo4jContentList = neo4jSearch.getContentIds();

                for (Object identity : neo4jContentList) {
                    String identifier = (String) identity;
                    if (contentIdentifier.contains(identifier)) {
                        contentIdentifier.remove(identifier);
                    }
                }

                neo4jContentList = null;

                int total = contentIdentifier.size();
                System.out.println("Deleting content data for count = " + total);

                int current = 0;
                long startTime = System.currentTimeMillis();
                while (current < total) {
                    int batch;
                    if (current + 50 <= total) {
                        batch = 50;
                    } else {
                        batch = total - current;
                    }

                    StringBuilder query = new StringBuilder("DELETE FROM content_store.content_data WHERE content_id IN (");
                    for (int i = 0; i < batch; i++) {
                        query.append("'");
                        query.append(contentIdentifier.get(current + i));
                        query.append("',");

                    }
                    query = query.deleteCharAt(query.lastIndexOf(","));
                    query.append(");");

//                System.out.println("Query to delete content hierarchy data : "+query);
                    ResultSet rs = session.execute(query.toString());
                    current += batch;
                    Progress.printProgress(startTime, total, current);
                }

//            printProgress(startTime, total, current);
                System.out.println("Successfully deleted data for contents not present in Neo4j.");
            }
        } catch (Exception e) {
            System.out.println("Failed to delete content data : " + e.getMessage());
            e.printStackTrace();
        }
    }

}
