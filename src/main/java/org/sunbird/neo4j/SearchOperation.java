package org.sunbird.neo4j;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.sunbird.util.logger.LoggerEnum;
import org.sunbird.util.logger.ProjectLogger;

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
//                    ProjectLogger.log("result : " + record.get("IDS").asObject());
                    ProjectLogger.log(
                            "Content ID Count : " + record.get("count").asLong(), LoggerEnum.INFO.name());
                    ids = record.get("contentids").asList();
//                    ProjectLogger.log(
//                            "Content ID is array : " + ids);
                    session.close();
                }
        } catch (Exception e) {
                ProjectLogger.log("Failed to fetch Content Ids from Neo4j.", e, LoggerEnum.ERROR.name());
            e.printStackTrace();
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
//                    ProjectLogger.log("result : " + record.get("IDS").asObject());
//                    ProjectLogger.log(
//                            "Content ID Count : " + record.get("count").asLong());
                String id = record.get("IDS").asString();
                String mimeType = record.get("MIME").asString();
                contentData.put(id, mimeType);
//                    ids = record.get("contentids").asList();
//                    ProjectLogger.log(
//                            "Content ID is array : " + ids);
            }
            session.close();
        } catch (Exception e) {
            ProjectLogger.log("Failed to fetch Content Ids from Neo4j.", e, LoggerEnum.ERROR.name());
            e.printStackTrace();
        }
        return contentData;
    }


    public int getCountForContentDataForAssets() {

        int assetCount = 0;
        Session session = null;
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content', 'ContentImage'] AND n.contentType IN ['Asset'] return count(*) AS COUNT;";
        try {
            session = ConnectionManager.getSession();
//                StatementResult result = session.run(query);
            StatementResult result = session.beginTransaction().run(query);
            while (result.hasNext()) {
                Record record = result.next();
                assetCount = record.get("COUNT").asInt();
                ProjectLogger.log("Total asset count to be migrated : " + assetCount, LoggerEnum.INFO.name());
            }
        } catch (Exception e) {
            ProjectLogger.log("Failed to fetch Count of Assets from Neo4j.", e, LoggerEnum.ERROR.name());
            e.printStackTrace();
        } finally {
            if(session != null) {
                session.close();
            }
        }
        return assetCount;
    }


    public Map<String, String> getContentDataForAssets(int skip, int size, Session session) {
        Map<String, String> contentData = new HashMap<>();
        String query = "MATCH (n) WHERE n.IL_FUNC_OBJECT_TYPE IN ['Content', 'ContentImage'] AND n.contentType IN ['Asset'] return n.mimeType AS MIME,n.downloadUrl AS URL ORDER BY id(n) SKIP %s LIMIT %s;";
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
            ProjectLogger.log("Failed to fetch Content Ids from Neo4j : " + e.getMessage(), e, LoggerEnum.ERROR.name());
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
//                    ProjectLogger.log("result : " + record.get("IDS").asObject());
//                    ProjectLogger.log(
//                            "Content ID Count : " + record.get("count").asLong());
                List ids = record.get("IDS").asList();
                String mimeType = record.get("MIME").asString();
                contentData.put(mimeType, ids);
//                    ids = record.get("contentids").asList();
//                    ProjectLogger.log(
//                            "Content ID is array : " + ids);
            }
            session.close();
        } catch (Exception e) {
            ProjectLogger.log("Failed to fetch Content Ids from Neo4j.", e, LoggerEnum.ERROR.name());
            e.printStackTrace();
        }
        return contentData;
    }


    public int getAllLiveContentCount(String contentType) {

        int liveContentCount = 0;
        Session session = null;
        String query = "MATCH (n) WHERE n.status IN ['Live', 'Failed'] AND NOT n.contentType IN ['Asset'] AND n.visibility='Default' AND n.contentType='%s' WITH count(*) AS COUNT return COUNT;";
        String formattedQuery = String.format(new String(query), contentType);
        try {
            session = ConnectionManager.getSession();
//                StatementResult result = session.run(query);
            StatementResult result = session.beginTransaction().run(formattedQuery);
            while (result.hasNext()) {
                Record record = result.next();
                liveContentCount = record.get("COUNT").asInt();
                ProjectLogger.log("Total Live Content count to be published : " + liveContentCount, LoggerEnum.INFO.name());
            }
        } catch (Exception e) {
            ProjectLogger.log("Failed to fetch count of Live Content from Neo4j.", e, LoggerEnum.ERROR.name());
            e.printStackTrace();
        } finally {
            if(session != null) {
                session.close();
            }
        }
        return liveContentCount;
    }

    public List getAllLiveContentIds(int skip, int size, String contentType) {
        List<String> ids = new ArrayList<>();
        Session session = ConnectionManager.getSession();
        String query = "MATCH (n) WHERE n.status IN ['Live', 'Failed'] AND NOT n.contentType IN ['Asset'] AND n.visibility='Default' AND n.contentType='%s' return n.IL_UNIQUE_ID AS contentids ORDER BY id(n) SKIP %s LIMIT %s;";
        String formattedQuery = String.format(new String(query), contentType, skip, size);
        try {
            StatementResult result = session.beginTransaction().run(formattedQuery);
            while (result.hasNext()) {
                Record record = result.next();
                String id = record.get("contentids").asString();
                ids.add(id);
            }
        } catch (Exception e) {
            ProjectLogger.log("Failed to fetch Content Ids from Neo4j.", e, LoggerEnum.ERROR.name());
            e.printStackTrace();
        } finally {
            if (session != null) {
                session.close();
            }
        }
        return ids;
    }

    public int getAllCollectionContentCount() {

        int collectionContentCount = 0;
        Session session = null;
        String query = "MATCH (n) WHERE n.mimeType='application/vnd.ekstep.content-collection' WITH count(*) AS COUNT return COUNT;";
        try {
            session = ConnectionManager.getSession();
//                StatementResult result = session.run(query);
            StatementResult result = session.beginTransaction().run(query);
            while (result.hasNext()) {
                Record record = result.next();
                collectionContentCount = record.get("COUNT").asInt();
                ProjectLogger.log("Total Content count to be migrated : " + collectionContentCount, LoggerEnum.INFO.name());
            }
        } catch (Exception e) {
            ProjectLogger.log("Failed to fetch count of Collection Content from Neo4j.", e, LoggerEnum.ERROR.name());
            e.printStackTrace();
        } finally {
            if(session != null) {
                session.close();
            }
        }
        return collectionContentCount;
    }

    public List getAllCollectionContentIds(int skip, int size) {
        List<String> ids = new ArrayList<>();
        Session session = ConnectionManager.getSession();
        String query = "MATCH (n) WHERE n.mimeType='application/vnd.ekstep.content-collection' return n.IL_UNIQUE_ID AS contentids ORDER BY id(n) SKIP %s LIMIT %s;";
        String formattedQuery = String.format(new String(query), skip, size);
        try {
            StatementResult result = session.beginTransaction().run(formattedQuery);
            while (result.hasNext()) {
                Record record = result.next();
                String id = record.get("contentids").asString();
                ids.add(id);
            }
        } catch (Exception e) {
            ProjectLogger.log("Failed to fetch Collection Content Ids from Neo4j.", e, LoggerEnum.ERROR.name());
            e.printStackTrace();
        } finally {
            if (session != null) {
                session.close();
            }
        }
        return ids;
    }
}
