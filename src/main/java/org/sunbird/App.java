package org.sunbird;

import org.neo4j.driver.v1.Session;
import org.sunbird.cassandra.DeleteOperation;
import org.sunbird.neo4j.ConnectionManager;
import org.sunbird.neo4j.ContentS3UrlUpdater;
import org.sunbird.neo4j.SearchOperation;
import org.sunbird.publish.Neo4jLiveContentPublisher;
import org.sunbird.s3.CopyObject;
import org.sunbird.s3.CopyObjectForAssets;
import org.sunbird.util.Progress;
//import org.sunbird.s3.CopyObjectThroughSDK;

import java.io.BufferedWriter;
import java.io.FileWriter;
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
            System.out.println("Enter 3 to perform S3 data migration for Neo4j Content except for ecml, html and h5p mimeType contents.");
            System.out.println("Enter 4 to perform S3 data migration for Neo4j Content for ecml, html and h5p mimeType only.");
            System.out.println("Enter 5 to perform S3 data migration for Neo4j Assets.");
            System.out.println("Enter 6 to update the S3 Urls of all the Neo4j Contents.");
            System.out.println("Enter 7 to Republish all Live contents of Neo4j.");
            System.out.println("Enter 8 to Republish specific content ids of Neo4j.");
            System.out.println("Enter 9 to EXIT");
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
                    boolean status = true;
                    boolean failStatus = false;
                    CopyObjectForAssets s3CopyAssets = new CopyObjectForAssets();
                    int skip = 0;
                    int size = 100;
                    String fileName = "Error_Asset_" + System.currentTimeMillis();
                    Session session = null;
                    try {
                        List<String> contentFailed;
                        int contentSize = operation.getCountForContentDataForAssets();

                        if (contentSize > 0) {
                            long startTime = System.currentTimeMillis();
                            session = ConnectionManager.getSession();
                            while (status) {
                                Map<String, String> contentDataForAssets = operation.getContentDataForAssets(skip, size, session);
                                contentFailed = s3CopyAssets.copyS3AssetDataForContentId(contentDataForAssets);
                                if(contentFailed.size() > 0) {
                                    appendToFile(contentFailed, fileName);
                                    failStatus = true;
                                }

                                Progress.printProgress(startTime, contentSize, (skip + contentDataForAssets.size()));

                                skip += size;

                                if(skip >= contentSize) {
                                    status = false;
                                }
                            }
                        } else {
                            System.out.println("No data of type Content or ContentImage in Neo4j.");
                        }

                        if(failStatus) {
                            System.out.println();
                            System.out.println("Failed for some content");
                            System.out.println("Please check the Error File");
                        } else {
                            System.out.println("Process completed Successfully for all Content of Neo4j.");
                        }
                    } catch (Exception e) {
                        System.out.println("Failed to fetch data from Neo4j due to : " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        if(session != null) {
                            session.close();
                        }
                    }
                    break;
                case 6:
                    ContentS3UrlUpdater updater = new ContentS3UrlUpdater();
                    List<Integer> failedId = updater.updateContentDataS3Urls();
                    if(failedId.size() > 0) {
                        System.out.println();
                        System.out.println("Failed for some ids");
                        writeNodeIdsTofile(failedId);
                    } else {
                        System.out.println("Process completed Successfully for all Content of Neo4j.");
                    }
                    break;
                case 7:
                    Neo4jLiveContentPublisher contentPublisher = new Neo4jLiveContentPublisher();
                    contentPublisher.publishAllContents();
                    break;
                case 8:
                    System.out.println("Enter content ids as comma separated values.");
                    String contents = scanner.nextLine();
                    String[] contentIds = contents.split(",");
                    if(contentIds.length > 0) {
                        Neo4jLiveContentPublisher contentPublisherForIds = new Neo4jLiveContentPublisher();
                        contentPublisherForIds.publishContentsForIds(contentIds);
                    } else {
                        System.out.println("Please enter some ids as comma separated values.");
                    }
                    break;
                case 9:
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

    public static void appendToFile(List<String> data, String fileName) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            writer.append(data.toString());

            writer.close();
        } catch (Exception e) {
            System.out.println("Failed to Write the File");
            e.printStackTrace();
        }
    }

    public static void writeNodeIdsTofile(List<Integer> data) {
        String fileName = "Error-S3URL-Update-Neo4j-" + System.currentTimeMillis();
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
