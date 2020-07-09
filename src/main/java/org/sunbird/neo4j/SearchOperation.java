package org.sunbird.neo4j;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SearchOperation {

    public List getContentIds() {
        List ids = null;
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content', 'ContentImage'] AND NOT n.contentType IN ['Asset'] WITH collect(n.IL_UNIQUE_ID) AS contentids, count(*) AS count return contentids,count;";
        try {
                Session session = ConnectionManager.getSession();
//                StatementResult result = session.run(query);
                StatementResult result = session.beginTransaction().run(query);
                while (result.hasNext()) {
                    Record record = result.next();
//                    System.out.println("result : " + record.get("IDS").asObject());
                    System.out.println(
                            "Content ID Count : " + record.get("count").asLong());
                    ids = record.get("contentids").asList();
//                    System.out.println(
//                            "Content ID is array : " + ids);
                    session.close();
                }
        } catch (Exception e) {
                System.out.println("Failed to fetch Content Ids from Neo4j.");
            }
        return ids;
    }

    public Map<String, String> getContentData() {
        Map<String, String> contentData = new HashMap<>();
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content', 'ContentImage'] AND NOT n.contentType IN ['Asset'] WITH n.IL_UNIQUE_ID AS IDS, n.mimeType AS MIME return DISTINCT IDS,MIME;";
        try {
            Session session = ConnectionManager.getSession();
//                StatementResult result = session.run(query);
            StatementResult result = session.beginTransaction().run(query);
            while (result.hasNext()) {
                Record record = result.next();
//                    System.out.println("result : " + record.get("IDS").asObject());
//                    System.out.println(
//                            "Content ID Count : " + record.get("count").asLong());
                String id = record.get("IDS").asString();
                String mimeType = record.get("MIME").asString();
                contentData.put(id, mimeType);
//                    ids = record.get("contentids").asList();
//                    System.out.println(
//                            "Content ID is array : " + ids);
            }
            session.close();
        } catch (Exception e) {
            System.out.println("Failed to fetch Content Ids from Neo4j.");
        }
        return contentData;
    }

    public List getAllLiveContentIds() {
        List ids = null;
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content'] AND n.status IN ['Live'] WITH collect(n.IL_UNIQUE_ID) AS contentids, count(*) AS count return contentids,count;";
        try {
            Session session = ConnectionManager.getSession();
//                StatementResult result = session.run(query);
            StatementResult result = session.beginTransaction().run(query);
            while (result.hasNext()) {
                Record record = result.next();
//                    System.out.println("result : " + record.get("IDS").asObject());
                System.out.println(
                        "Content ID Count : " + record.get("count").asLong());
                ids = record.get("contentids").asList();
//                    System.out.println(
//                            "Content ID is array : " + ids);
                session.close();
            }
        } catch (Exception e) {
            System.out.println("Failed to fetch Content Ids from Neo4j.");
        }
        return ids;
    }
}
