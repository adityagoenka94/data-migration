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
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content', 'ContentImage'] AND NOT n.contentType IN ['Asset'] AND NOT n.mimeType IN ['application/vnd.ekstep.h5p-archive','application/vnd.ekstep.html-archive','application/vnd.ekstep.ecml-archive','text/x-url','video/x-youtube','application/vnd.ekstep.content-collection'] WITH n.IL_UNIQUE_ID AS IDS, n.mimeType AS MIME return DISTINCT IDS,MIME;";
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


    public int getCountForContentDataForAssets() {

        int assetCount = 0;
        Session session = null;
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content', 'ContentImage'] AND n.contentType IN ['Asset'] AND NOT n.mimeType IN ['application/vnd.ekstep.h5p-archive','application/vnd.ekstep.html-archive','application/vnd.ekstep.ecml-archive','text/x-url','video/x-youtube','application/vnd.ekstep.content-collection'] return count(*) AS COUNT;";
        try {
            session = ConnectionManager.getSession();
//                StatementResult result = session.run(query);
            StatementResult result = session.beginTransaction().run(query);
            while (result.hasNext()) {
                Record record = result.next();
                assetCount = record.get("COUNT").asInt();
                System.out.println("Total asset count to be migrated : " + assetCount);
            }
        } catch (Exception e) {
            System.out.println("Failed to fetch Count of Assets from Neo4j.");
        } finally {
            if(session != null) {
                session.close();
            }
        }
        return assetCount;
    }


    public Map<String, String> getContentDataForAssets(int skip, int size, Session session) {
        Map<String, String> contentData = new HashMap<>();
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content', 'ContentImage'] AND n.contentType IN ['Asset'] AND NOT n.mimeType IN ['application/vnd.ekstep.h5p-archive','application/vnd.ekstep.html-archive','application/vnd.ekstep.ecml-archive','text/x-url'] return n.mimeType AS MIME,n.downloadUrl AS URL ORDER BY id(n) SKIP %s LIMIT %s;";
        String formattedQuery = String.format(new String(query), skip, size);
        try {
//                StatementResult result = session.run(query);
            StatementResult result = session.beginTransaction().run(formattedQuery);
            while (result.hasNext()) {
                Record record = result.next();
                String mime = record.get("MIME").asString();
                String downloadUrl = record.get("URL").asString();
                contentData.put(downloadUrl, mime);
            }
        } catch (Exception e) {
            System.out.println("Failed to fetch Content Ids from Neo4j : " + e.getMessage());
            e.printStackTrace();

        }
        return contentData;
    }


    public Map<String, List> getContentDataForMimes() {
        Map<String, List> contentData = new HashMap<>();
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content', 'ContentImage'] AND n.mimeType IN ['application/vnd.ekstep.h5p-archive','application/vnd.ekstep.html-archive','application/vnd.ekstep.ecml-archive'] WITH n.mimeType AS MIME,collect( DISTINCT n.IL_UNIQUE_ID) AS IDS return MIME,IDS;";
        try {
            Session session = ConnectionManager.getSession();
//                StatementResult result = session.run(query);
            StatementResult result = session.beginTransaction().run(query);
            while (result.hasNext()) {
                Record record = result.next();
//                    System.out.println("result : " + record.get("IDS").asObject());
//                    System.out.println(
//                            "Content ID Count : " + record.get("count").asLong());
                List ids = record.get("IDS").asList();
                String mimeType = record.get("MIME").asString();
                contentData.put(mimeType, ids);
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


    public int getAllLiveContentCount() {

        int liveContentCount = 0;
        Session session = null;
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content'] AND n.status IN ['Live'] WITH count(*) AS COUNT return COUNT;";
        try {
            session = ConnectionManager.getSession();
//                StatementResult result = session.run(query);
            StatementResult result = session.beginTransaction().run(query);
            while (result.hasNext()) {
                Record record = result.next();
                liveContentCount = record.get("COUNT").asInt();
                System.out.println("Total Live Content count to be published : " + liveContentCount);
            }
        } catch (Exception e) {
            System.out.println("Failed to fetch count of Live Content from Neo4j.");
        } finally {
            if(session != null) {
                session.close();
            }
        }
        return liveContentCount;
    }

    public List getAllLiveContentIds(int skip, int size, Session session) {
        List ids = null;
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content'] AND n.status IN ['Live'] return n.IL_UNIQUE_ID AS contentids ORDER BY id(n) SKIP %s LIMIT %s;";
        String formattedQuery = String.format(new String(query), skip, size);
        try {
            session = ConnectionManager.getSession();
            StatementResult result = session.beginTransaction().run(formattedQuery);
            while (result.hasNext()) {
                Record record = result.next();
                String id = record.get("contentids").asString();
                ids.add(id);
            }
        } catch (Exception e) {
            System.out.println("Failed to fetch Content Ids from Neo4j.");
        }
        return ids;
    }
}
