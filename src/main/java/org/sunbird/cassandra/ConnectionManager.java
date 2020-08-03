package org.sunbird.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.sunbird.util.PropertiesCache;
import org.sunbird.util.logger.LoggerEnum;
import org.sunbird.util.logger.ProjectLogger;

import java.net.InetSocketAddress;
import java.util.Arrays;

public class ConnectionManager {

    static Cluster cluster;
    private static PropertiesCache propertiesCache;
    private static Session session;

    static {
        propertiesCache = PropertiesCache.getInstance();
        try {
            cluster = Cluster.builder()
                    .addContactPointsWithPorts(Arrays.asList(
                            new InetSocketAddress(propertiesCache.getProperty("cassandra_host"), Integer.parseInt(propertiesCache.getProperty("cassandra_port")))))
                    .withoutJMXReporting()
                    .build();
            ProjectLogger.log("Connecting to cassandra db", LoggerEnum.INFO.name());
            session = cluster.connect();
            ProjectLogger.log("Connected to cassandra db", LoggerEnum.INFO.name());
            shutDownHook();
        } catch (NoHostAvailableException e) {
            ProjectLogger.log("Exception occured while connecting to the host ; " + e.getMessage(), e, LoggerEnum.ERROR.name());
            System.exit(-1);
        } catch (Exception ex) {
            ProjectLogger.log("Failed to connect to cassandra : " + ex.getMessage(), ex, LoggerEnum.ERROR.name());
            System.exit(-1);
        }
    }

    public static Session getSession() {
        return session;
    }

    public static void shutDownHook() {
        Runtime runtime = Runtime.getRuntime();
        runtime.addShutdownHook(new CassandraConnection());
    }

    static class CassandraConnection extends Thread {
        @Override
        public void run() {
            ProjectLogger.log("Killing Cassandra Connection.", LoggerEnum.INFO.name());
            try {
                if (session != null) {
                    session.close();
                    cluster.close();
                }
            } catch (Exception e) {
                ProjectLogger.log("failed to kill Cassandra Connection : " + e.getMessage(), e, LoggerEnum.ERROR.name());
            }
        }
    }
}
