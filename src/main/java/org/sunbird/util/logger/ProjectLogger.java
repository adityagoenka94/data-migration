package org.sunbird.util.logger;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.sunbird.util.PropertiesCache;

import java.io.IOException;
import java.util.Properties;

public class ProjectLogger {

    final static Logger logger;
    static PropertiesCache propertiesCache = PropertiesCache.getInstance();

    static {
        logger = Logger.getLogger(ProjectLogger.class);
        try {
            Properties p = new Properties();
            p.load(ProjectLogger.class.getResourceAsStream("/logger/log4j.properties"));
            String logPath = propertiesCache.getProperty("log_path");

            if(logPath!=null && logPath.length() > 0) {
                logPath += "/data-migration.log";
                System.out.println("Log Path : " + logPath);
                p.setProperty("log4j.appender.file.File", logPath);
            }
            PropertyConfigurator.configure(p);
        }
        catch (IOException e) {

            ProjectLogger.log("Unable to read log4j.configuration file",e, LoggerEnum.ERROR.name());
//            throw new Exception("Missing Log4j Configuration File : log4j.properties");
        }
    }

    public static Logger getProjectLogger() {
        return logger;
    }

    public static void log(String message, String logLevel) {


        switch(logLevel) {
            case "INFO":
                logger.info(message);
                break;
            case "WARN":
                logger.warn(message);
                break;
            case "DEBUG":
                logger.debug(message);
                break;
            case "ERROR":
                logger.error(message);
                break;
            case "FATAL":
                logger.fatal(message);
                break;
                default:
                logger.debug(message);
        }
    }

    public static void log(String message,Throwable t, String logLevel) {


        switch(logLevel) {
            case "INFO":
                logger.info(message,t);
                break;
            case "WARN":
                logger.warn(message,t);
                break;
            case "DEBUG":
                logger.debug(message,t);
                break;
            case "ERROR":
                logger.error(message,t);
                break;
            case "FATAL":
                logger.fatal(message);
                break;
            default:
                logger.debug(message,t);
        }
    }

}
