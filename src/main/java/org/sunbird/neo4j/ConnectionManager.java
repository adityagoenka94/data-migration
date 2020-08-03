package org.sunbird.neo4j;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.sunbird.util.PropertiesCache;
import org.sunbird.util.logger.LoggerEnum;
import org.sunbird.util.logger.ProjectLogger;

public class ConnectionManager {

//    private static Connection connection;
    private static PropertiesCache propertiesCache;
    private static Driver driver;

    static {
        propertiesCache = PropertiesCache.getInstance();
        ProjectLogger.log("Creating connection with the Neo4j Database", LoggerEnum.INFO.name());
        // Checking if the Neo4j Url is provided
        String url = null;
        Config config = Config.builder()
                .withTrustStrategy( Config.TrustStrategy.trustAllCertificates())
                .withoutEncryption()
                .build();

        if (propertiesCache.getProperty("neo4j_url") != null) {
            try {
                try {
                    // Trying to create connection with an open Neo4j database
                    url = propertiesCache.getProperty("neo4j_url");
                    driver = GraphDatabase.driver(url, config);
//                    connection = DriverManager.getConnection(url);
//                    connection.setReadOnly(true);
                    ProjectLogger.log("Connection created with the Neo4j Database successfully", LoggerEnum.INFO.name());
                } catch (AuthenticationException e) {
//                    connection = DriverManager.getConnection(url, propertiesCache.getProperty("neo4j_username"), propertiesCache.getProperty("neo4j_password"));
                    driver = GraphDatabase.driver(
                            url, AuthTokens.basic(propertiesCache.getProperty("neo4j_username"), propertiesCache.getProperty("neo4j_password")),config);
//                    connection.setReadOnly(true);
                    ProjectLogger.log("Connection created with the Neo4j Database successfully", LoggerEnum.INFO.name());
                }
            } catch (AuthenticationException e) {
                ProjectLogger.log("Unable to connect to Neo4j Database", e, LoggerEnum.ERROR.name());
                e.printStackTrace();
            } catch (Exception e) {
                ProjectLogger.log("Unable to connect to Neo4j Database", e, LoggerEnum.ERROR.name());
                e.printStackTrace();
            }
        } else {
            ProjectLogger.log("neo4j_url variable missing. Cannot connect to database.", LoggerEnum.INFO.name());
            System.exit(-1);
        }
        registerShutdownHook();
    }

    public static Session getSession() {
        if (driver != null) {
            return driver.session();
        } else {
            return null;
        }
    }

    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ProjectLogger.log("Closing Neo4j Graph Driver...", LoggerEnum.INFO.name());
                try {
                    if (null != driver)
                        driver.close();
                } catch (Exception e) { }
            }
        });
    }

}