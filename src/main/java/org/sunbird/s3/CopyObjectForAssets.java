package org.sunbird.s3;

import org.sunbird.util.PropertiesCache;
import org.sunbird.util.logger.LoggerEnum;
import org.sunbird.util.logger.ProjectLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class CopyObjectForAssets {


    private List<String> commandFailed = new ArrayList<>();
    private static PropertiesCache propertiesCache = PropertiesCache.getInstance();
    private String[] mimeTypesNotToHandle = new String[]{
            "text/x-url"};
    List<String> notMime = Arrays.asList(mimeTypesNotToHandle);


    public List<String> copyS3AssetDataForContentId(Map<String, String> contentData) {

        String awsCommand = getAwsCommandForAssetMigration();
//        ProjectLogger.log("AWS build command : " + awsCommand);
        ExecutorService executor = Executors.newFixedThreadPool(20);
        if(awsCommand != null) {
            List<Future<Boolean>> status = new ArrayList<>();
            for (Map.Entry<String, String> entry : contentData.entrySet()) {
                String downloadUrl = entry.getKey();
                String mime = entry.getValue();
                String command = new String(awsCommand);
//                ProjectLogger.log("Download Url : " + downloadUrl);
                String commandToRun = getS3UrlForAssets(command, downloadUrl, mime);
                if(!commandToRun.isEmpty())
                    status.add(executor.submit(new CallableThread(commandToRun)));
            }

            try {
                int statusSize = status.size();
                for(int i=0; i < statusSize; i++) {
                    boolean response = status.get(i).get();
                }
            } catch (InterruptedException | ExecutionException e) {
                ProjectLogger.log("Exception occurred while waiting for the result : " + e.getMessage(), e, LoggerEnum.ERROR.name());
                e.printStackTrace();
            }
        } else {
            ProjectLogger.log("Please initialize the S3 variables properly.", LoggerEnum.INFO.name());
        }
        this.awaitTerminationAfterShutdown(executor);
        return commandFailed;
    }

    private void awaitTerminationAfterShutdown(ExecutorService threadPool) {

        try {
            threadPool.shutdown();
        } catch (Exception ex) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
            ProjectLogger.log("An error occurred while shutting down the Executor Service : " + ex.getMessage(), ex, LoggerEnum.ERROR.name());
            ex.printStackTrace();
        }
    }

    private String getAwsCommandForAssetMigration() {
        StringBuilder awsCommand = new StringBuilder();
        String s3bucketFrom = propertiesCache.getProperty("source_s3bucket");
        String regionFrom = propertiesCache.getProperty("source_region");
        String s3bucketTo = propertiesCache.getProperty("destination_s3bucket");
        String regionTo = propertiesCache.getProperty("destination_region");

        awsCommand.append("aws s3 cp");
        if(regionFrom != null && !regionFrom.isEmpty()) {
            awsCommand.append(" --source-region ").append(regionFrom);
        }

        if(regionTo != null && !regionTo.isEmpty()) {
            awsCommand.append(" --region ").append(regionTo);
        }

        awsCommand.append(" ");

        if(s3bucketFrom == null || s3bucketFrom.isEmpty() || s3bucketTo == null || s3bucketTo.isEmpty()) {
            return null;
        } else {
            awsCommand.append("\"");
            awsCommand.append(s3bucketFrom);

            awsCommand.append("%s");
            awsCommand.append("\"");

            awsCommand.append(" ");

            awsCommand.append("\"");
            awsCommand.append(s3bucketTo);
            awsCommand.append("%s");
            awsCommand.append("\"");
        }

        awsCommand.append(" --recursive");
//        awsCommand.append(" --acl public-read");
        return awsCommand.toString();
    }


    public String getS3UrlForAssets(String command, String downloadUrl, String mimeType) {

        String newCommand = "";

        if(downloadUrl != null) {
            try {
                downloadUrl = URLDecoder.decode(downloadUrl, StandardCharsets.UTF_8.toString());
            } catch (UnsupportedEncodingException e) {
                commandFailed.add(downloadUrl);
            }
        }


        if(downloadUrl == null) {
            return "";
        } else if(notMime.contains(mimeType)) {
            return "";
        } else if(downloadUrl.startsWith("https://sl-content-migration.s3.amazonaws.com/assets/")) {
            return "";
        } else if(downloadUrl.startsWith("https://sl-content-migration.s3.amazonaws.com/content/assets/")) {
            return "";
        } else if(!downloadUrl.startsWith("https://sl-content-migration.s3.amazonaws.com/")) {
            return "";
        } else {
            String contentSubUrl = downloadUrl.replaceAll("https://sl-content-migration.s3.amazonaws.com/","");
            int index = contentSubUrl.indexOf("/", contentSubUrl.indexOf("/") + 1);
            if(contentSubUrl.startsWith("content/do_")) {
//                ProjectLogger.log("1" + contentSubUrl);
                if(index > 0) {
                    String contentFolderUrl = contentSubUrl.substring(0, index);
                    newCommand = String.format(command, contentFolderUrl, contentFolderUrl);
                } else {
                    newCommand = String.format(command, contentSubUrl, contentSubUrl);
                }
//                ProjectLogger.log("2" + contentSubUrl);
            } else if(index > 0) {
                String contentFolderUrl = contentSubUrl.substring(0, index);
                newCommand = String.format(command, contentFolderUrl, contentFolderUrl);
            }
            else if (contentSubUrl.startsWith("content/")) {
//                ProjectLogger.log("3" + contentSubUrl);
                newCommand = String.format(command, contentSubUrl, contentSubUrl);
                newCommand = newCommand.replace("--recursive" ,"");
            } else if (contentSubUrl.startsWith("media/")) {
//                ProjectLogger.log("4" + contentSubUrl);
                newCommand = String.format(command, contentSubUrl, contentSubUrl);
                newCommand = newCommand.replace("--recursive" ,"");
            } else {
//                ProjectLogger.log("No Match");
            }
        }

        return newCommand;
    }

    private boolean runS3ShellCommand(String command) {
        String result = "";
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command("/bin/sh", "-c", command);
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();
//            ProjectLogger.log("Command Generated : " + command);
//            ProjectLogger.log("running the request");
//            Process process = runtime.exec(new String[] {"/bin/sh", "-c", command});
//            while (process.isAlive()) {
//                Thread.sleep(1000);
//            }
            result = getResult(process);
            int exitVal = process.waitFor();
            if(exitVal == 0) {
//                ProjectLogger.log("received exit code 0");
//                result = getResult(process);
                boolean status = verifyCurrentContentMigration(result);
                return true;
            }  else {
//                ProjectLogger.log("received exit code not 0");
//                result = getResult(process);
                ProjectLogger.log("Failed for command : " + command, LoggerEnum.INFO.name());
                throw new Exception("Command terminated abnormally : " + result);
            }
        }
//        catch (IOException e) {
//            addAllContentIdsForFailedList(currentContentIds);
//        } catch (InterruptedException e) {
//            addAllContentIdsForFailedList(currentContentIds);
//        }
        catch (Exception e) {
            ProjectLogger.log("Some error occurred while running the aws command : " + e.getMessage(), e, LoggerEnum.ERROR.name());
            e.printStackTrace();
            commandFailed.add(command);
//            addAllContentIdForFailedList(currentContentIds);
        }
        return false;
    }

    private String getResult(Process process) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = "";
        StringBuilder result = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }

//        ProjectLogger.log(result.toString());

        return result.toString();
    }

    private boolean verifyCurrentContentMigration(String result) {
        if (!result.isEmpty() && result !=null) {
            return true;
        } else {
//            failedForContent.add(contentId);
            return false;
        }
    }


    class CallableThread implements Callable<Boolean> {

        private String commandToRun;

        public CallableThread(String commandToRun) {
            this.commandToRun = commandToRun;
        }


        @Override
        public Boolean call() {
            try {
//                ProjectLogger.log("Command : " + commandToRun);
                return runS3ShellCommand(commandToRun);
//                return true;


            } catch (Exception e) {
                ProjectLogger.log("Some error occurred while running the aws script.", e, LoggerEnum.ERROR.name());
                return false;
            }
        }
    }

}
