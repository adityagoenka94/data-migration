package org.sunbird;

import org.sunbird.cassandra.DeleteOperation;
import org.sunbird.neo4j.SearchOperation;
import org.sunbird.publish.Neo4jLiveContentPublisher;
import org.sunbird.s3.CopyObject;
//import org.sunbird.s3.CopyObjectThroughSDK;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
        SearchOperation operation = new SearchOperation();
        CopyObject s3CopyObject = new CopyObject();

        boolean check = true;
        while(check) {
            System.out.println("Enter 1 to get Content Id list from Neo4j");
            System.out.println("Enter 2 to filter content and framework data from Cassandra.");
            System.out.println("Enter 3 to perform S3 data migration for Neo4j Content except for ecml, html and h5p mimeType.");
            System.out.println("Enter 4 to perform S3 data migration for Neo4j Content for ecml, html and h5p mimeType only.");
            System.out.println("Enter 5 to Republish all Live contents of Neo4j.");
            System.out.println("Enter 6 to EXIT");
//        System.out.println("Enter 5 to perform data migration for specific Content Ids Using SDK");
            Scanner scanner = new Scanner(System.in);

            int option = scanner.nextInt();
            scanner.nextLine();

            Map<String, String> contentData;
            switch(option) {
                case 1:
                    System.out.println("Enter CSV file Path");
                    String filePath = scanner.nextLine();
                    FileWriter fileWriter = new FileWriter(filePath);
                    PrintWriter printWriter = new PrintWriter(fileWriter);
                    contentData = operation.getContentData();

                    for (Map.Entry<String,String> entry : contentData.entrySet()) {
                        printWriter.println(entry.getKey() + "," + entry.getValue());
                    }
                    printWriter.close();
                    fileWriter.close();
                    System.out.println("Completed");
                    break;
                case 2 :
                    DeleteOperation deleteOperation = new DeleteOperation();
                    deleteOperation.deleteFrameworkHierarchy();
                    deleteOperation.deleteContentHierarchy();
                    deleteOperation.deleteContentData();
                    break;
                case 3:
                    contentData = operation.getContentData();
                    if(contentData.size() > 0) {
                        List<String> contentFailed = s3CopyObject.copyS3ContentDataForContentIdV2(contentData);
                        if(contentFailed.size() > 0) {
                            System.out.println();
                            System.out.println("Failed for some content");
                            writeTofile(contentFailed);
                        } else {
                            System.out.println("Process completed Successfully for all Content of Neo4j.");
                        }
                    }
                    else {
                        System.out.println("Neo4j has no Content.");
                    }
                    break;
                case 4:
                    Map<String, List> contentDataForMime = operation.getContentDataForMimes();
                    if(contentDataForMime.size() > 0) {
                        List<String> contentFailed = s3CopyObject.copyS3ContentDataForMimes(contentDataForMime);
                        if(contentFailed.size() > 0) {
                            System.out.println();
                            System.out.println("Failed for some content");
                            writeTofile(contentFailed);

                        } else {
                            System.out.println("Process completed Successfully for all Content of Neo4j.");
                        }
                    }
                    else {
                        System.out.println("Neo4j has no Content.");
                    }
                    break;
                case 5:
                    Neo4jLiveContentPublisher contentPublisher = new Neo4jLiveContentPublisher();
                    contentPublisher.publishAllContents();
                    break;
                case 6:
                    System.out.println();
                    System.out.println("Bye Bye !!");
                    check = false;
                    break;
//                case 5:
//                    org.sunbird.cassandra.SearchOperation searchOperation = new org.sunbird.cassandra.SearchOperation();
//                    searchOperation.getAllContentDataIdentifier();
//                    break;
//            case 3:
//                contentIds = getInputContentIds(scanner);
//                if(contentIds.size() > 0) {
//                    CopyObjectThroughSDK copyObjectThroughSDK = new CopyObjectThroughSDK();
//                    failedContent = copyObjectThroughSDK.copyS3ContentDataForContentIds(contentIds);
//                    if(failedContent.size() > 0) {
//                        System.out.println("Failed for content with IDS : " + failedContent);
//                    } else {
//                        System.out.println("Process completed Successfully for all Content Ids.");
//                    }
//                } else {
//                    System.out.println("Process provide some content Ids.");
//                }
                default:
                    System.out.println("Wrong option selected.");
                    check = false;
            }
            if (check) {
                System.out.println();
                System.out.println("*************************************************************************");
                System.out.println();
                System.out.println();
            }

        }
        System.exit(0);
    }

    public static List<String> getInputContentIds(Scanner scanner) {
        System.out.println("Enter Content ids as comma seperated values");
        String contntId = scanner.nextLine();
        String[] contents = contntId.trim().split(",");
        List<String> contentIds = Arrays.asList(contents);
        if(contentIds.size() > 0) {
            return contentIds;
        } else {
            return null;
        }
    }

    public static void writeTofile(List<String> data) {
        String fileName = "Error_" + System.currentTimeMillis();
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            writer.write(data.toString());

            writer.close();
        } catch (Exception e) {
            System.out.println("Failed to Write the File");
            e.printStackTrace();
        }
    }
}
