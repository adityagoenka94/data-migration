package org.sunbird;

import org.neo4j.driver.v1.Session;
import org.sunbird.cassandra.DeleteOperation;
import org.sunbird.hierarchy.Neo4jToCassandraHierarchyManager;
import org.sunbird.neo4j.ConnectionManager;
import org.sunbird.neo4j.ContentS3UrlUpdater;
import org.sunbird.neo4j.SearchOperation;
import org.sunbird.publish.Neo4jLiveContentPublisher;
import org.sunbird.s3.CopyObject;
import org.sunbird.s3.CopyObjectForAssets;
import org.sunbird.util.Progress;
import org.sunbird.util.logger.LoggerEnum;
import org.sunbird.util.logger.ProjectLogger;
//import org.sunbird.s3.CopyObjectThroughSDK;

import java.io.*;
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
            ProjectLogger.log("Enter 1 to get Content Id list from Neo4j", LoggerEnum.INFO.name());
            ProjectLogger.log("Enter 2 to filter content and framework data from Cassandra.", LoggerEnum.INFO.name());
            ProjectLogger.log("Enter 3 to perform S3 data migration for Neo4j Content except for ecml, html and h5p mimeType contents.", LoggerEnum.INFO.name());
            ProjectLogger.log("Enter 4 to perform S3 data migration for Neo4j Content for ecml, html and h5p mimeType only.", LoggerEnum.INFO.name());
            ProjectLogger.log("Enter 5 to perform S3 data migration for Neo4j Assets.", LoggerEnum.INFO.name());
            ProjectLogger.log("Enter 6 to update the S3 Urls of all the Neo4j Contents.", LoggerEnum.INFO.name());
            ProjectLogger.log("Enter 7 to Republish all Live contents of Neo4j.", LoggerEnum.INFO.name());
            ProjectLogger.log("Enter 8 to Republish specific content ids of Neo4j.", LoggerEnum.INFO.name());
            ProjectLogger.log("Enter 9 to Migrate Hierarchy of Contents form Cassandra to Neo4j.", LoggerEnum.INFO.name());
            ProjectLogger.log("Enter 10 to EXIT", LoggerEnum.INFO.name());
//        ProjectLogger.log("Enter 5 to perform data migration for specific Content Ids Using SDK");
            Scanner scanner = new Scanner(System.in);

            int option = scanner.nextInt();
            scanner.nextLine();

            Map<String, String> contentData;
            switch(option) {
                case 1:
                    ProjectLogger.log("Enter CSV file Path", LoggerEnum.INFO.name());
                    String filePath = scanner.nextLine();
                    FileWriter fileWriter = new FileWriter(filePath);
                    PrintWriter printWriter = new PrintWriter(fileWriter);
                    contentData = operation.getContentData();

                    for (Map.Entry<String,String> entry : contentData.entrySet()) {
                        printWriter.println(entry.getKey() + "," + entry.getValue());
                    }
                    printWriter.close();
                    fileWriter.close();
                    ProjectLogger.log("Completed", LoggerEnum.INFO.name());
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
                            ProjectLogger.log("", LoggerEnum.INFO.name());
                            ProjectLogger.log("Failed for some content", LoggerEnum.INFO.name());
                            writeTofile(contentFailed);
                        } else {
                            ProjectLogger.log("Process completed Successfully for all Content of Neo4j.", LoggerEnum.INFO.name());
                        }
                    }
                    else {
                        ProjectLogger.log("Neo4j has no Content.", LoggerEnum.INFO.name());
                    }
                    break;
                case 4:
                    Map<String, List> contentDataForMime = operation.getContentDataForMimes();
                    if(contentDataForMime.size() > 0) {
                        List<String> contentFailed = s3CopyObject.copyS3ContentDataForMimes(contentDataForMime);
                        if(contentFailed.size() > 0) {
                            ProjectLogger.log("", LoggerEnum.INFO.name());
                            ProjectLogger.log("Failed for some content", LoggerEnum.INFO.name());
                            writeTofile(contentFailed);

                        } else {
                            ProjectLogger.log("Process completed Successfully for all Content of Neo4j.", LoggerEnum.INFO.name());
                        }
                    }
                    else {
                        ProjectLogger.log("Neo4j has no Content.", LoggerEnum.INFO.name());
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
                            ProjectLogger.log("No data of type Content or ContentImage in Neo4j.", LoggerEnum.INFO.name());
                        }

                        if(failStatus) {
                            ProjectLogger.log("", LoggerEnum.INFO.name());
                            ProjectLogger.log("Failed for some content", LoggerEnum.INFO.name());
                            ProjectLogger.log("Please check the Error File", LoggerEnum.INFO.name());
                        } else {
                            ProjectLogger.log("Process completed Successfully for all Content of Neo4j.", LoggerEnum.INFO.name());
                        }
                    } catch (Exception e) {
                        ProjectLogger.log("Failed to fetch data from Neo4j due to : " + e.getMessage(), e, LoggerEnum.ERROR.name());
//                        e.printStackTrace();
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
                        ProjectLogger.log("", LoggerEnum.INFO.name());
                        ProjectLogger.log("Failed for some ids", LoggerEnum.INFO.name());
                        writeNodeIdsTofile(failedId);
                    } else {
                        ProjectLogger.log("Process completed Successfully for all Content of Neo4j.", LoggerEnum.INFO.name());
                    }
                    break;
                case 7:
                    Neo4jLiveContentPublisher contentPublisher = new Neo4jLiveContentPublisher();
                    contentPublisher.publishAllContents();
                    break;
                case 8:
                    ProjectLogger.log("Enter File Path.", LoggerEnum.INFO.name());
                    String contents = "";
                    String FileName = "";
                    try {
                        fileName = scanner.nextLine();

//                    BufferedReader ob = new BufferedReader(new InputStreamReader(new FileInputStream(contents)));
                        BufferedReader br = new BufferedReader(new FileReader(fileName));

                        //One way of reading the file
                        String contentLine = br.readLine();
                        while (contentLine != null) {
                            contents += contentLine;
                            contentLine = br.readLine();
                        }
                        br.close();
                        if (contents.contains("[")) {
                            contents = contents.replace("[", "");
                        }
                        if (contents.contains("]")) {
                            contents = contents.replace("]", "");
                        }
                    } catch (Exception e) {
                        ProjectLogger.log("Failed ro format provided contents due to : " + e.getMessage(), e, LoggerEnum.ERROR.name());
                        e.printStackTrace();
                    }
                    String[] contentIds = contents.split(",");
                    if(contentIds.length > 0) {
                        Neo4jLiveContentPublisher contentPublisherForIds = new Neo4jLiveContentPublisher();
                        contentPublisherForIds.verifyProperties();
                        contentPublisherForIds.publishContentsForIds(contentIds);
                    } else {
                        ProjectLogger.log("Please enter some ids as comma separated values.", LoggerEnum.INFO.name());
                    }
                    break;
                case 9:
                    Neo4jToCassandraHierarchyManager hierarchyManager = new Neo4jToCassandraHierarchyManager();
                    hierarchyManager.migrateAllContentsHierarchy();
                    break;
                case 10:
                    ProjectLogger.log("", LoggerEnum.INFO.name());
                    ProjectLogger.log("Bye Bye !!", LoggerEnum.INFO.name());
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
//                        ProjectLogger.log("Failed for content with IDS : " + failedContent);
//                    } else {
//                        ProjectLogger.log("Process completed Successfully for all Content Ids.");
//                    }
//                } else {
//                    ProjectLogger.log("Process provide some content Ids.");
//                }
                default:
                    ProjectLogger.log("Wrong option selected.", LoggerEnum.INFO.name());
                    check = false;
            }
            if (check) {
                ProjectLogger.log("", LoggerEnum.INFO.name());
                ProjectLogger.log("*************************************************************************", LoggerEnum.INFO.name());
                ProjectLogger.log("", LoggerEnum.INFO.name());
                ProjectLogger.log("", LoggerEnum.INFO.name());
            }

        }
        System.exit(0);
    }

    public static List<String> getInputContentIds(Scanner scanner) {
        ProjectLogger.log("Enter Content ids as comma seperated values", LoggerEnum.INFO.name());
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
            ProjectLogger.log("Failed to Write the File", e, LoggerEnum.ERROR.name());
            e.printStackTrace();
        }
    }

    public static void appendToFile(List<String> data, String fileName) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            writer.append(data.toString());

            writer.close();
        } catch (Exception e) {
            ProjectLogger.log("Failed to Write the File", e,  LoggerEnum.ERROR.name());
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
            ProjectLogger.log("Failed to Write the File", e,  LoggerEnum.ERROR.name());
            e.printStackTrace();
        }
    }

}
