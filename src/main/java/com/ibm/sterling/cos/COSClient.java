package com.ibm.sterling.cos;

import java.io.InputStream;
import java.util.List;

public interface COSClient {
	
	public List<String> listObjects(String bucketName, int numOfObjects);
	
	public void writeObject(String bucketName, String objKeyName, InputStream is);
	
	public InputStream readObject(String bucketName, String objKeyName) ;
	
	public void setEndpointURL(String url);
	
	public void setServiceResourceId(String serviceResourceId);
	
	public void setApiKey(String apiKey);
	
	public void setLocation(String location);
	
	public void copyObject(String fromBucket, String keyName, String toBucket,  String toKeyName);
	
	
	public void deleteObject(String fromBucket,  String keyName);
	
	public void moveObject(String bucketName, String keyName, String toBucket, String toKeyName);

	public void shutDown();

}
