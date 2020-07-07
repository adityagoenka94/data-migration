package org.sunbird.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.sunbird.util.PropertiesCache;

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
            System.out.println("Connecting to cassandra db");
            session = cluster.connect();
            System.out.println("Connected to cassandra db");
            shutDownHook();
        } catch (NoHostAvailableException e) {
            System.out.println("Exception occured while connecting to the host ; " + e.getMessage());
            System.exit(-1);
        } catch (Exception ex) {
            System.out.println("Failed to connect to cassandra : " + ex.getMessage());
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
            System.out.println("Killing Cassandra Connection.");
            try {
                if (session != null) {
                    session.close();
                    cluster.close();
                }
            } catch (Exception e) {
                System.out.println("failed to kill Cassandra Connection : " + e.getMessage());
            }
        }
    }
}
