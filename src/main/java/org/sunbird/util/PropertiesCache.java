package org.sunbird.util;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesCache {

    private static final Properties properties = new Properties();
    private static PropertiesCache propertiesCache;
    private final String[] propertiesFileName = new String[]{
            "/application.properties"};


    private PropertiesCache() {

        for(int i=0; i<propertiesFileName.length; i++) {
            InputStream inputStream = this.getClass().getResourceAsStream(propertiesFileName[i]);
            try {
                properties.load(inputStream);
            }
            catch (IOException e)
            {
                System.out.println("Error while reading Files.");
            }
        }


    }


    public void saveConfigProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    public String getProperty(String key) {
        String value = System.getenv(key);
        if (value != null && !value.isEmpty()) {
            return value;
        } else {
            return properties.getProperty(key) != null ? properties.getProperty(key) : key;
        }
    }



    public static PropertiesCache getInstance() {
        if (null == propertiesCache) {
            propertiesCache = new PropertiesCache();
        }
        return propertiesCache;
    }
}
