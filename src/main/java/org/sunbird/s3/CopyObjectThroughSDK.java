//package org.sunbird.s3;
//
//import org.sunbird.util.PropertiesCache;
//import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
//import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
//import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.s3.S3Client;
//import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
//import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
//import software.amazon.awssdk.services.s3.model.S3Exception;
//
//import java.io.UnsupportedEncodingException;
//import java.net.URLEncoder;
//import java.nio.charset.StandardCharsets;
//import java.util.ArrayList;
//import java.util.List;
//
//public class CopyObjectThroughSDK {
//    List<String> failedForContent = new ArrayList<>();
//    private static PropertiesCache propertiesCache = PropertiesCache.getInstance();
//
//    public List<String> copyS3ContentDataForContentIds(List ids) {
//
//        //Create the S3Client object
//        String s3bucketFrom = propertiesCache.getProperty("source_s3bucket");
//        String s3FolderFrom = propertiesCache.getProperty("source_s3folder");
//        String regionFrom = propertiesCache.getProperty("source_region");
//        String s3bucketTo = propertiesCache.getProperty("destination_s3bucket");
//        String s3FolderTo = propertiesCache.getProperty("destination_s3folder");
//        String regionTo = propertiesCache.getProperty("destination_region");
//
////        AwsSessionCredentials awsCreds = AwsSessionCredentials.create(
////                propertiesCache.getProperty("accesskey"),
////                propertiesCache.getProperty("secretkey"),
////                "your_session_token_here");
////
////        S3Client s3 = S3Client.builder()
////                .credentialsProvider(StaticCredentialsProvider.create())
////                .region(Region.of(regionTo))
////                .build();
//
//        S3Client s3 = S3Client.builder()
//                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
//                .region(Region.of(regionTo))
//                .build();
//
//        copyBucketObject(s3, s3bucketFrom, ids, s3bucketTo, s3FolderFrom, s3FolderTo);
//
//        return failedForContent;
//    }
//
//    public void copyBucketObject (S3Client s3, String fromBucket, List objectKey, String toBucket, String s3FolderFrom, String s3FolderTo) {
//
//        String encodedUrl = null;
//
////        for(Object key: objectKey) {
////            String keyValue = (String) key;
//            try {
//                encodedUrl = URLEncoder.encode(fromBucket + "/" + s3FolderFrom, StandardCharsets.UTF_8.toString());
//            } catch (UnsupportedEncodingException e) {
//                failedForContent.add(s3FolderFrom);
//                System.out.println("URL could not be encoded: " + e.getMessage());
//            }
//            CopyObjectRequest copyReq = CopyObjectRequest.builder()
//                    .copySource(encodedUrl)
//                    .destinationBucket(toBucket)
//                    .destinationKey(s3FolderTo)
//                    .build();
//
//            try {
//                CopyObjectResponse copyRes = s3.copyObject(copyReq);
//                System.out.println(copyRes.copyObjectResult().toString());
//            } catch (S3Exception e) {
//                failedForContent.add(s3FolderFrom);
//                System.err.println(e.awsErrorDetails().errorMessage());
//                System.exit(1);
//            }
////        }
//    }
//    }
