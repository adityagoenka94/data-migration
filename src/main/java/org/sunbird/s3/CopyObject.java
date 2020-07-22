package org.sunbird.s3;

import org.sunbird.util.Progress;
import org.sunbird.util.PropertiesCache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;

public class CopyObject {

    private Set<String> failedForContent = new TreeSet<>();
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

    private Runtime runtime = Runtime.getRuntime();


    public List<String> copyS3ContentDataForContentIdV2(Map<String, String> contentData) {

        String awsCommand = getAwsCommandForContentIdFolderMigrationV2();
        System.out.println("AWS build command : " + awsCommand);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        if(awsCommand != null) {
            int total = contentData.size();
            long startTime = System.currentTimeMillis();
            List<Future<Boolean>> status = new ArrayList<>();
            Map<String, String> commandList = new HashMap<>();
            for(Map.Entry<String,String> entry : contentData.entrySet()) {
                String contentId = entry.getKey();
                String mimeType = entry.getValue();

                String command = new String(awsCommand);
                String commandToRun = getContentFolderUrl(command, contentId, mimeType);
                if(!commandToRun.isEmpty()) {
                    commandList.put(contentId, commandToRun);
                }
            }


            for(Map.Entry<String,String> entry : commandList.entrySet()) {
                String contentId = entry.getKey();
                String commandToRun = entry.getValue();
                try {
                    status.add(executor.submit(new CallableThread(commandToRun, contentId)));
//                    runS3ShellCommand(commandToRun, new String[]{id});

                } catch (Exception e) {
                    System.out.println("Failed for the command : " + commandToRun);
                    System.out.println(e.getMessage());
                }
            }

            try {
                for(int i=0; i < status.size(); i++) {
                    boolean response = status.get(i).get();
                    Progress.printProgress(startTime, total, i+1);
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


    private String getContentFolderUrl(String command, String id, String mimeType) {
        String newCommand = "";
        if(! notMime.contains(mimeType)) {
                newCommand = String.format(command, id, id);
            }
        return newCommand;
    }


    private String getAwsCommandForContentIdFolderMigrationV2() {
        StringBuilder awsCommand = new StringBuilder();
        String s3bucketFrom = propertiesCache.getProperty("source_s3bucket");
        String s3FolderFrom = propertiesCache.getProperty("source_s3folder");
        String regionFrom = propertiesCache.getProperty("source_region");
        String s3bucketTo = propertiesCache.getProperty("destination_s3bucket");
        String s3FolderTo = propertiesCache.getProperty("destination_s3folder");
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
            awsCommand.append(s3bucketFrom);
            if(s3FolderFrom !=null && !s3FolderFrom.isEmpty()) {
                awsCommand.append(s3FolderFrom);
            }

            awsCommand.append("%s/");
            awsCommand.append(" ");

            awsCommand.append(s3bucketTo);
            if(s3FolderTo !=null && !s3FolderTo.isEmpty()) {
                awsCommand.append(s3FolderTo);
            }
            awsCommand.append("%s/");
        }

        awsCommand.append(" --recursive");
//        awsCommand.append(" --acl public-read");
        return awsCommand.toString();
    }

    private boolean runS3ShellCommand(String command, String currentContentIds) {
        String result = "";
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command("/bin/sh", "-c", command);
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();
//            System.out.println("Command Generated : " + command);
            result = getResult(process);
            int exitVal = process.waitFor();
            if(exitVal == 0) {
//                boolean status = verifyCurrentContentMigration(result, currentContentIds);
                return true;
            }  else {
                throw new Exception("Command terminated abnormally : " + result);
            }
        }
        catch (Exception e) {
            System.out.println("Some error occurred while running the aws command : " + e.getMessage());
            e.printStackTrace();
            commandFailed.add(command);
            addAllContentIdForFailedList(currentContentIds);
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

    private boolean verifyCurrentContentMigration(String result, String contentId) {
        if (result.contains(contentId)) {
            return true;
        } else {
            failedForContent.add(contentId);
            return false;
        }
    }

    private void addAllContentIdForFailedList(String contentId) {
            failedForContent.add(contentId);
    }


    class CallableThread implements Callable<Boolean> {

        private String id;
        private String commandToRun;

        public CallableThread(String commandToRun, String id) {
            this.commandToRun = commandToRun;
            this.id = id;
        }


        @Override
        public Boolean call() {
            try {
//                System.out.println("Command : " + commandToRun);
                return runS3ShellCommand(commandToRun, id);
//                return true;


            } catch (Exception e) {
                System.out.println("Some error occurred while running the aws script.");
                return false;
            }
        }
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


    public List<String> copyS3ContentDataForMimes(Map<String, List> contentData) {

        try {
            String awsCommand = getAwsCommandForContentIdFolderMigrationV2();
            System.out.println("AWS build command : " + awsCommand);
            ExecutorService executor = Executors.newFixedThreadPool(5);
            if (awsCommand != null) {
//            int total = contentData.size();
                long startTime = System.currentTimeMillis();
                List<Future<Boolean>> status = new ArrayList<>();
//            Map<String, String> commandList = new HashMap<>();
                for (Map.Entry<String, List> entry : contentData.entrySet()) {
                    String mimeType = entry.getKey();
//                    System.out.println("Making request for MimeType : " + mimeType);
                    List ids = entry.getValue();
                    for (Object iterator : ids) {
                        String id = (String) iterator;
                        String command = new String(awsCommand);
//                        System.out.println("Making request for id : " + id);
//                        new MimeCallableThread(command, id, mimeType).run();
                        status.add(executor.submit(new MimeCallableThread(command, id, mimeType)));
                    }
                }


                try {
                    int statusSize = status.size();
                    for (int i = 0; i < statusSize; i++) {
                        boolean response = status.get(i).get();
                        Progress.printProgress(startTime, statusSize, i + 1);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    System.out.println("Exception occurred while waiting for the result : " + e.getMessage());
                    e.printStackTrace();
                }
            } else {
                System.out.println("Please initialize the S3 variables properly.");
            }
            this.awaitTerminationAfterShutdown(executor);
        } catch (Exception e) {
            System.out.println("Failed in copyS3ContentDataForMimes : " + e.getMessage());
            e.printStackTrace();
        }
        return commandFailed;
    }

    private String getContentS3UrlForMime(String command, String id, String mimeType, String iterator) {
        String newCommand = "";
        String folder = "";
        switch (mimeType) {
            case "application/vnd.ekstep.ecml-archive":
                folder = "ecml/" + id + "-" + iterator;
                break;
            case "application/vnd.ekstep.html-archive":
                folder = "html/" + id + "-" + iterator;
                break;
            case "application/vnd.ekstep.h5p-archive":
                folder = "h5p/" + id + "-" + iterator;
                break;
            default:
                return "";
        }
        newCommand = String.format(command, folder, folder);
        return newCommand;
    }

    class MimeCallableThread implements Callable<Boolean> {

        private String id;
        private String command;
        private String mimeType;

        public MimeCallableThread(String command, String id, String mimeType) {
            this.command = command;
            this.id = id;
            this.mimeType = mimeType;
        }


        @Override
        public Boolean call() {
            try {
                int i=1;
                boolean status = true;
                while(status) {
                    String folderId = i + ".0";
                    String commandToRun = getContentS3UrlForMime(command, id, mimeType, folderId);
                    System.out.println("Command : " + commandToRun);
                    boolean check = runS3ShellCommand(commandToRun, id);
//                    check = false;
                    if(check) {
                        i++;
                    } else {
                        status = false;
                    }
                }

                String commandToRun = getContentS3UrlForMime(command, id, mimeType, "latest");
                System.out.println("Command : " + commandToRun);
                boolean check1 = runS3ShellCommand(commandToRun, id);
//                boolean check1 = true;
                commandToRun = getContentS3UrlForMime(command, id, mimeType, "snapshot");
                System.out.println("Command : " + commandToRun);
                boolean check2 = runS3ShellCommand(commandToRun, id);

                return check1;
            } catch (Exception e) {
                System.out.println("Some error occurred while running the aws script.");
                return false;
            }
        }
    }

}
