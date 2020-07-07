package org.sunbird.s3;

import org.sunbird.util.PropertiesCache;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CopyObject {

    List<String> failedForContent = new ArrayList<>();
    private static PropertiesCache propertiesCache = PropertiesCache.getInstance();


    Runtime runtime = Runtime.getRuntime();
//    public List<String> copyS3ContentDataForContentIds(List ids) {
//
//        String awsCommand = getAwsCommandForContentIdFolderMigration();
//        System.out.println("AWS build command : " + awsCommand);
//        int total = ids.size();
//        int current = 0;
//        long startTime = System.currentTimeMillis();
//        while(current < total) {
//            int batch;
//            if(current+10 <= total) {
//                batch = 10;
//            } else {
//                batch = total - current;
//            }
//
//            StringBuilder command = new StringBuilder(awsCommand);
//            String currentContents[] = new String[batch];
//            for(int i = 0; i < batch; i++) {
//                currentContents[i] = (String)ids.get(current+i);
//                command.append(" --include '").append((String)ids.get(current+i)).append( "*'");
//            }
//
//            try {
//                runS3ShellCommand(command.toString(), currentContents);
//
//            } catch (Exception e) {
//                System.out.println("Failed for the command : " + command.toString());
//                System.out.println(e.getMessage());
//            }
//            current += batch;
//            printProgress(startTime, total, current);
//        }
//        return failedForContent;
//    }


    public List<String> copyS3ContentDataForContentIdV2(List ids) {

        String awsCommand = getAwsCommandForContentIdFolderMigrationV2();
        System.out.println("AWS build command : " + awsCommand);
        if(awsCommand != null) {
            int total = ids.size();
            long startTime = System.currentTimeMillis();
            for(int i=0; i < total; i++) {
                String id = (String)ids.get(i);
                String command = new String(awsCommand);
                String commandToRun = String.format(command, id, id);

                try {
                    runS3ShellCommand(commandToRun, new String[]{id});

                } catch (Exception e) {
                    System.out.println("Failed for the command : " + command.toString());
                    System.out.println(e.getMessage());
                }
                printProgress(startTime, total, i+1);
            }
        } else {
            System.out.println("Please initialize the S3 variables properly.");
        }
        return failedForContent;
    }

//    public String getAwsCommandForContentIdFolderMigration() {
//        StringBuilder awsCommand = new StringBuilder();
//        String s3bucketFrom = propertiesCache.getProperty("source_s3bucket");
//        String s3FolderFrom = propertiesCache.getProperty("source_s3folder");
//        String regionFrom = propertiesCache.getProperty("source_region");
//        String s3bucketTo = propertiesCache.getProperty("destination_s3bucket");
//        String s3FolderTo = propertiesCache.getProperty("destination_s3folder");
//        String regionTo = propertiesCache.getProperty("destination_region");
//        awsCommand.append("aws s3 cp ");
//
//        if(s3bucketFrom == null || s3bucketFrom.isEmpty() || s3bucketTo == null || s3bucketTo.isEmpty()) {
//            return null;
//        } else {
//            awsCommand.append(s3bucketFrom);
//            if(s3FolderFrom !=null && !s3FolderFrom.isEmpty()) {
//                awsCommand.append(s3FolderFrom);
//            }
//
//            awsCommand.append(" ");
//
//            awsCommand.append(s3bucketTo);
//            if(s3FolderTo !=null && !s3FolderTo.isEmpty()) {
//                awsCommand.append(s3FolderTo);
//            }
//        }
//
//        if(regionFrom != null && !regionFrom.isEmpty()) {
//            awsCommand.append(" --source-region " + regionFrom);
//        }
//
//        if(regionTo != null && !regionTo.isEmpty()) {
//            awsCommand.append(" --region " + regionTo);
//        }
//
//        awsCommand.append(" --recursive");
//        return awsCommand.toString();
//    }

    public String getAwsCommandForContentIdFolderMigrationV2() {
        StringBuilder awsCommand = new StringBuilder();
        String s3bucketFrom = propertiesCache.getProperty("source_s3bucket");
        String s3FolderFrom = propertiesCache.getProperty("source_s3folder");
        String regionFrom = propertiesCache.getProperty("source_region");
        String s3bucketTo = propertiesCache.getProperty("destination_s3bucket");
        String s3FolderTo = propertiesCache.getProperty("destination_s3folder");
        String regionTo = propertiesCache.getProperty("destination_region");

        awsCommand.append("aws s3 cp ");
        if(regionFrom != null && !regionFrom.isEmpty()) {
            awsCommand.append(" --source-region " + regionFrom);
        }

        if(regionTo != null && !regionTo.isEmpty()) {
            awsCommand.append(" --region " + regionTo);
        }

        awsCommand.append(" ");

        if(s3bucketFrom == null || s3bucketFrom.isEmpty() || s3bucketTo == null || s3bucketTo.isEmpty()) {
            return null;
        } else {
            awsCommand.append(s3bucketFrom);
            if(s3FolderFrom !=null && !s3FolderFrom.isEmpty()) {
                awsCommand.append(s3FolderFrom);
            }

            awsCommand.append("%s");
            awsCommand.append(" ");

            awsCommand.append(s3bucketTo);
            if(s3FolderTo !=null && !s3FolderTo.isEmpty()) {
                awsCommand.append(s3FolderTo);
            }
            awsCommand.append("%s");
        }

        awsCommand.append(" --recursive");
        return awsCommand.toString();
    }

    public void runS3ShellCommand(String command, String[] currentContentIds) {
        String result = "";
        try {
//            System.out.println("Command Generated : " + command);
            Process process = runtime.exec(new String[] {"/bin/sh", "-c",command});
            int exitVal = process.waitFor();
            if(exitVal == 0) {
                result = getResult(process);
                verifyCurrentContentMigration(result, currentContentIds);
            }  else {
                result = getResult(process);
                throw new Exception("Command terminated abnormally : " + result);
            }

        } catch (IOException e) {
            addAllContentIdsForFailedList(currentContentIds);
        } catch (InterruptedException e) {
            addAllContentIdsForFailedList(currentContentIds);
        } catch (Exception e) {
            addAllContentIdsForFailedList(currentContentIds);
        }
    }

    public String getResult(Process process) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = "";
        StringBuilder result = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }

//        System.out.println(result.toString());
        return result.toString();
    }

    public void verifyCurrentContentMigration(String result, String[] currentContentIds) {
        for(String contentId : currentContentIds) {
            if (result.indexOf(contentId) >= 0) {

            } else {
                failedForContent.add(contentId);
            }
        }
    }

    public void addAllContentIdsForFailedList(String[] currentContentIds) {
        for(String contentId : currentContentIds) {
            failedForContent.add(contentId);
        }
    }

    private static void printProgress(long startTime, long total, long current) {
        long eta = current == 0 ? 0 :
                (total - current) * (System.currentTimeMillis() - startTime) / current;

        String etaHms = current == 0 ? "N/A" :
                String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(eta),
                        TimeUnit.MILLISECONDS.toMinutes(eta) % TimeUnit.HOURS.toMinutes(1),
                        TimeUnit.MILLISECONDS.toSeconds(eta) % TimeUnit.MINUTES.toSeconds(1));

        StringBuilder string = new StringBuilder(140);
        int percent = (int) (current * 100 / total);
        string
                .append('\r')
                .append(String.join("", Collections.nCopies(percent == 0 ? 2 : 2 - (int) (Math.log10(percent)), " ")))
                .append(String.format(" %d%% [", percent))
                .append(String.join("", Collections.nCopies(percent, "=")))
                .append('>')
                .append(String.join("", Collections.nCopies(100 - percent, " ")))
                .append(']')
                .append(String.join("", Collections.nCopies((int) (Math.log10(total)) - (int) (Math.log10(current)), " ")))
                .append(String.format(" %d/%d, ETA: %s", current, total, etaHms));

        System.out.print(string);
    }
}
