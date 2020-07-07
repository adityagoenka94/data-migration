package org.sunbird.neo4j;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.AuthenticationException;
import org.sunbird.util.PropertiesCache;

public class ConnectionManager {

//    private static Connection connection;
    private static PropertiesCache propertiesCache;
    private static Driver driver;

    static {
        propertiesCache = PropertiesCache.getInstance();
        System.out.println("Creating connection with the Neo4j Database");
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
                    System.out.println("Connection created with the Neo4j Database successfully");
                } catch (AuthenticationException e) {
//                    connection = DriverManager.getConnection(url, propertiesCache.getProperty("neo4j_username"), propertiesCache.getProperty("neo4j_password"));
                    driver = GraphDatabase.driver(
                            url, AuthTokens.basic(propertiesCache.getProperty("neo4j_username"), propertiesCache.getProperty("neo4j_password")),config);
//                    connection.setReadOnly(true);
                    System.out.println("Connection created with the Neo4j Database successfully");
                }
            } catch (AuthenticationException e) {
                System.out.println("Unable to connect to Neo4j Database");
                e.printStackTrace();
            } catch (Exception e) {
                System.out.println("Unable to connect to Neo4j Database");
                e.printStackTrace();
            }
        } else {
            System.out.println("neo4j_url variable missing. Cannot connect to database.");
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
                System.out.println("Closing Neo4j Graph Driver...");
                try {
                    if (null != driver)
                        driver.close();
                } catch (Exception e) { }
            }
        });
    }

}