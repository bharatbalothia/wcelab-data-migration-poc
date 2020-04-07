package com.ibm.sterling.cos;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.SDKGlobalConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.model.GetObjectRequest;
import com.ibm.cloud.objectstorage.services.s3.model.ListObjectsV2Request;
import com.ibm.cloud.objectstorage.services.s3.model.ListObjectsV2Result;
import com.ibm.cloud.objectstorage.services.s3.model.S3Object;
import com.ibm.cloud.objectstorage.services.s3.model.S3ObjectSummary;

public class COSClientImpl implements COSClient {

	private AmazonS3 _cosClient;

	private String apiKey = null;
	private String serviceResourceId = null;
	private String endpointURL = null;
	private String location = "us";

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public COSClientImpl() {
		SDKGlobalConfiguration.IAM_ENDPOINT = "https://iam.cloud.ibm.com/identity/token";
		
	}
	
	public void init() {
		this._cosClient = createClient();
	}

	public String getServiceResourceId() {
		return serviceResourceId;
	}

	public void setServiceResourceId(String serviceResourceId) {
		this.serviceResourceId = serviceResourceId;
	}

	public String getEndpointURL() {
		return endpointURL;
	}

	public void setEndpointURL(String endpointURL) {
		this.endpointURL = endpointURL;
	}

	public void setApiKey(String apiKey) {
		this.apiKey = apiKey;
	}

	@Override
	public List<String> listObjects(String bucketName, int numOfObjects) {
		
		List<String> keyList = new ArrayList<String>();
		boolean moreResults = true;
		String nextToken = "";

		while (moreResults) {
			ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucketName)
					.withMaxKeys(numOfObjects).withContinuationToken(nextToken);
			ListObjectsV2Result result = getCosClient().listObjectsV2(request);
			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				keyList.add(objectSummary.getKey());
			}
			if (result.isTruncated()) {
				nextToken = result.getNextContinuationToken();
			} else {
				nextToken = "";
				moreResults = false;
			}
		}

		return keyList;
	}

	private AmazonS3 getCosClient() {
		if(null == this._cosClient) {
			this.init();
		}
		return _cosClient;
	}

	@Override
	public void writeObject(String bucketName, String objKeyName, InputStream is) {
		throw new UnsupportedOperationException("Not yet implemented");

	}

	@Override
	public InputStream readObject(String bucketName, String objKeyName) {
		if (StringUtils.isNotBlank(bucketName) && StringUtils.isNotBlank(objKeyName)) {
			GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, objKeyName);

			S3Object s3Object = getCosClient().getObject(getObjectRequest);

			return s3Object.getObjectContent();
		} else {
			throw new IllegalArgumentException("Either bucketName or the objKeyName is blank");
		}

	}

	private AmazonS3 createClient() {
		if(StringUtils.isBlank(apiKey)) {
			throw new IllegalStateException("API Key is not set..");
		}
		if(StringUtils.isBlank(this.serviceResourceId)) {
			throw new IllegalStateException("Service Resorce ID is not set..");
		}
		
		if(StringUtils.isBlank(this.getEndpointURL())) {
			throw new IllegalStateException("Endpoint URL is not set..");
		}
		
		if(StringUtils.isBlank(this.getLocation())) {
			throw new IllegalStateException("Location is not set..");
		}
		
		AWSCredentials credentials;
		credentials = new BasicIBMOAuthCredentials(this.apiKey, this.serviceResourceId);

		ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(5000);
		clientConfig.setUseTcpKeepAlive(true);

		AmazonS3 cosClient = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withEndpointConfiguration(new EndpointConfiguration(this.getEndpointURL(), this.getLocation()))
				.withPathStyleAccessEnabled(true).withClientConfiguration(clientConfig).build();
		return cosClient;
	}

	
	public void copyObject(String fromBucket, String keyName, String toBucket,  String toKeyName) {
		this.getCosClient().copyObject(fromBucket, keyName, toBucket, toKeyName);
		
	}

	
	public void deleteObject(String bucketName, String keyName) {
		this.getCosClient().deleteObject(bucketName, keyName);
		
	}
	
	public void moveObject(String bucketName, String keyName, String toBucket, String toKeyName) {
		this.copyObject(bucketName, keyName, toBucket, toKeyName);
		this.deleteObject(bucketName, keyName);
	}


}
