package org.sunbird.s3;

import org.sunbird.util.PropertiesCache;

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
            "application/vnd.ekstep.h5p-archive",
            "application/vnd.ekstep.html-archive",
            "application/vnd.ekstep.ecml-archive",
            "text/x-url",
            "video/x-youtube",
            "application/vnd.ekstep.content-collection"};
    List<String> notMime = Arrays.asList(mimeTypesNotToHandle);


    public List<String> copyS3AssetDataForContentId(Map<String, String> contentData) {

        String awsCommand = getAwsCommandForAssetMigration();
//        System.out.println("AWS build command : " + awsCommand);
        ExecutorService executor = Executors.newFixedThreadPool(20);
        if(awsCommand != null) {
            int total = contentData.size();
            long startTime = System.currentTimeMillis();
            List<Future<Boolean>> status = new ArrayList<>();
            Map<String, String> commandList = new HashMap<>();
            for (Map.Entry<String, String> entry : contentData.entrySet()) {
                String downloadUrl = entry.getKey();
//                    System.out.println("Making request for MimeType : " + mimeType);
                String mime = entry.getValue();
                String command = new String(awsCommand);
//                System.out.println("Download Url : " + downloadUrl);
                String commandToRun = getS3UrlForAssets(command, downloadUrl, mime);
//                System.out.println("Command to Run : " + commandToRun);
//                        new MimeCallableThread(command, id, mimeType).run();
                if(!commandToRun.isEmpty())
                    status.add(executor.submit(new CallableThread(commandToRun)));
            }

            try {
                int statusSize = status.size();
                for(int i=0; i < statusSize; i++) {
                    boolean response = status.get(i).get();
                }
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("Exception occurred while waiting for the result : " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("Please initialize the S3 variables properly.");
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
            System.out.println("An error occurred while shutting down the Executor Service : " + ex.getMessage());
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
            if(contentSubUrl.startsWith("content/do_")) {
//                System.out.println("1" + contentSubUrl);
                int index = contentSubUrl.indexOf("/", contentSubUrl.indexOf("/") + 1);
                String contentFolderUrl = contentSubUrl.substring(0, index);
                newCommand = String.format(command, contentFolderUrl, contentFolderUrl);
//                System.out.println("2" + contentSubUrl);
            } else if (contentSubUrl.startsWith("content/")) {
//                System.out.println("3" + contentSubUrl);
                newCommand = String.format(command, contentSubUrl, contentSubUrl);
                newCommand = newCommand.replace("--recursive" ,"");
            } else if (contentSubUrl.startsWith("media/")) {
//                System.out.println("4" + contentSubUrl);
                newCommand = String.format(command, contentSubUrl, contentSubUrl);
                newCommand = newCommand.replace("--recursive" ,"");
            } else {
//                System.out.println("No Match");
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
//            System.out.println("Command Generated : " + command);
//            System.out.println("running the request");
//            Process process = runtime.exec(new String[] {"/bin/sh", "-c", command});
//            while (process.isAlive()) {
//                Thread.sleep(1000);
//            }
            result = getResult(process);
            int exitVal = process.waitFor();
            if(exitVal == 0) {
//                System.out.println("received exit code 0");
//                result = getResult(process);
//                boolean status = verifyCurrentContentMigration(result);
                return true;
            }  else {
//                System.out.println("received exit code not 0");
//                result = getResult(process);
                throw new Exception("Command terminated abnormally : " + result);
            }
        }
//        catch (IOException e) {
//            addAllContentIdsForFailedList(currentContentIds);
//        } catch (InterruptedException e) {
//            addAllContentIdsForFailedList(currentContentIds);
//        }
        catch (Exception e) {
            System.out.println("Some error occurred while running the aws command : " + e.getMessage());
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

//        System.out.println(result.toString());

        return result.toString();
    }

//    private boolean verifyCurrentContentMigration(String result, String contentId) {
//        if (result.contains(contentId)) {
//            return true;
//        } else {
//            failedForContent.add(contentId);
//            return false;
//        }
//    }


    class CallableThread implements Callable<Boolean> {

        private String commandToRun;

        public CallableThread(String commandToRun) {
            this.commandToRun = commandToRun;
        }


        @Override
        public Boolean call() {
            try {
                System.out.println("Command : " + commandToRun);
                return runS3ShellCommand(commandToRun);
//                return true;


            } catch (Exception e) {
                System.out.println("Some error occurred while running the aws script.");
                return false;
            }
        }
    }

}
