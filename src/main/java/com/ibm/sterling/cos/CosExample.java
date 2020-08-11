package com.ibm.sterling.cos;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.List;
import java.util.Random;

import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.SDKGlobalConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.model.Bucket;
import com.ibm.cloud.objectstorage.services.s3.model.CreateBucketRequest;
import com.ibm.cloud.objectstorage.services.s3.model.ListObjectsRequest;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectListing;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cloud.objectstorage.services.s3.model.S3ObjectSummary;

public class CosExample {

	private static AmazonS3 _cosClient;

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		SDKGlobalConfiguration.IAM_ENDPOINT = "https://iam.cloud.ibm.com/identity/token";

		//String bucketName = "orderdash-donotdelete-pr-p0hrpkzixacuzi"; // eg my-unique-bucket-name
		String newBucketName = "oms-migration"; // eg my-other-unique-bucket-name
		String api_key = "xfUjUq-KczSK__JF-GwrDR71EB2GzVdYwT8W4vtfycRG"; // eg
																			// "W00YiRnLW4k3fTjMB-oiB-2ySfTrFBIQQWanc--P3byk"
		String service_instance_id = "crn:v1:bluemix:public:cloud-object-storage:global:a/33c5711b8afbf7fd809a4529de35532a:1859fde0-b540-43bd-b7ea-22ed80991835::";
		String endpoint_url = "s3.us-east.cloud-object-storage.appdomain.cloud"; // this could be any service
																					// endpoint

		String location = "us";

		System.out.println("Current time: " + new Timestamp(System.currentTimeMillis()).toString());
		_cosClient = createClient(api_key, service_instance_id, endpoint_url, location);

		//listObjects("order-bridge", _cosClient);
		// createBucket("testbucket", _cosClient);
		//listBuckets(_cosClient);

		try {
			String keyName = "object_no_";
			for (int i = 0; i < 200; i++) {
				String objName = keyName+i;
				System.out.println("ObjectName:"+objName);
				
				writeObject(newBucketName, _cosClient, objName);
				
			}
			
			System.out.println("Done writing..");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * @param api_key
	 * @param service_instance_id
	 * @param endpoint_url
	 * @param location
	 * @return AmazonS3
	 */
	public static AmazonS3 createClient(String api_key, String service_instance_id, String endpoint_url,
			String location) {
		AWSCredentials credentials;
		credentials = new BasicIBMOAuthCredentials(api_key, service_instance_id);

		ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(5000);
		clientConfig.setUseTcpKeepAlive(true);

		AmazonS3 cosClient = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withEndpointConfiguration(new EndpointConfiguration(endpoint_url, location))
				.withPathStyleAccessEnabled(true).withClientConfiguration(clientConfig).build();
		return cosClient;
	}

	/**
	 * @param bucketName
	 * @param cosClient
	 */
	public static void listObjects(String bucketName, AmazonS3 cosClient) {
		System.out.println("Listing objects in bucket " + bucketName);
		ObjectListing objectListing = cosClient.listObjects(new ListObjectsRequest().withBucketName(bucketName));
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			
			System.out.println(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
		}
		System.out.println();
	}

	/**
	 * @param bucketName
	 * @param cosClient
	 * @param storageClass
	 */
	public static void createBucket(String bucketName, AmazonS3 cosClient) {

		// CreateBucketRequest cbReq = new CreateBucketRequest(bucketName);
		CreateBucketRequest cbReq = new CreateBucketRequest(bucketName, "us-east");
		cosClient.createBucket(cbReq);
	}

	public static void writeObject(String bucketName, AmazonS3 cosClient, String objName) throws IOException {
		String obj = getRandomString(); // the object to be stored
		ByteArrayOutputStream theBytes = new ByteArrayOutputStream(); // create a new output stream to store the object
				
		theBytes.write(obj.getBytes());// data
		
		InputStream stream = new ByteArrayInputStream(theBytes.toByteArray()); // convert the serialized data to a new
																				// input stream to store
		ObjectMetadata metadata = new ObjectMetadata(); // define the metadata
		metadata.setContentType("application/text"); // set the metadata
		metadata.setContentLength(theBytes.size()); // set metadata for the length of the data stream
		cosClient.putObject(bucketName, // the name of the bucket to which the object is being written
				objName, // the name of the object being written
				stream, // the name of the data stream writing the object
				metadata // the metadata for the object being written
		);
	}

	/**
	 * @param cosClient
	 */
	public static void listBuckets(AmazonS3 cosClient) {
		System.out.println("Listing buckets");
		final List<Bucket> bucketList = _cosClient.listBuckets();
		for (final Bucket bucket : bucketList) {
			System.out.println(bucket.getName());
		}
		
		
		System.out.println();
	}

	
	public static String getRandomString() {
		int leftLimit = 48; // numeral '0'
	    int rightLimit = 122; // letter 'z'
	    int targetStringLength = 50;
	    Random random = new Random();
	 
	    String generatedString = random.ints(leftLimit, rightLimit + 1)
	      .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
	      .limit(targetStringLength)
	      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
	      .toString();
	    System.out.println("Random String:"+generatedString);
	    return generatedString;
	}
}
