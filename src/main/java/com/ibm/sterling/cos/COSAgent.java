package com.ibm.sterling.cos;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Document;

import com.yantra.ycp.japi.util.YCPBaseAgent;
import com.yantra.yfc.dom.YFCDocument;
import com.yantra.yfc.dom.YFCElement;
import com.yantra.yfc.log.YFCLogCategory;
import com.yantra.yfc.log.YFCLogUtil;
import com.yantra.yfs.core.YFSSystem;
import com.yantra.yfs.japi.YFSEnvironment;

public class COSAgent extends YCPBaseAgent {

	private static final String OBJECT_KEY = "ObjectKey";
	private static final String LOCATION = "Location";
	private static final YFCLogCategory logger = YFCLogCategory.instance(COSAgent.class.getName());
	private static final String INPUT_BUCKET_NAME = "InputBucketName";
	private static final String SERVICE_RESOURCE_ID = "ServiceResourceId";
	private static final String ENDPOINT_URL = "EndpointURL";
	private static final String API_KEY_NAME = "ApiKeyName";
	private static final String ERROR_BUCKET_NAME = "ErrorBucketName";
	private static final String SUCCESS_BUCKET_NAME = "SuccessBucketName";

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List getJobs(YFSEnvironment env, Document criteriaDoc, Document lastMessage) {
		logger.beginTimer("getJobs");
		COSClient cosClient = new COSClientImpl();
		try {
			if (YFCLogUtil.isDebugEnabled()) {
				logger.debug("getJobs Input:" + YFCDocument.getDocumentFor(criteriaDoc).getString());
			}
			List jobList = new ArrayList();
			/*
			 * if (null != lastMessage) { logger.info("Last Message:" +
			 * YFCDocument.getDocumentFor(lastMessage).getString()); return jobList; }
			 */
			

			YFCDocument critDoc = YFCDocument.getDocumentFor(criteriaDoc);

			YFCElement rootEl = critDoc.getDocumentElement();

			String propApiKeyName = rootEl.getAttribute(API_KEY_NAME);
			String apiKey = YFSSystem.getProperties().getProperty(propApiKeyName);
			String endpointUrl = rootEl.getAttribute(ENDPOINT_URL);
			String serviceResourceId = rootEl.getAttribute(SERVICE_RESOURCE_ID);
			String location = rootEl.getAttribute(LOCATION);
			String bucketName = rootEl.getAttribute(INPUT_BUCKET_NAME);

			int numOfRecords = rootEl.getIntAttribute("NumRecordsToBuffer", 100);

			cosClient.setApiKey(apiKey);
			cosClient.setEndpointURL(endpointUrl);
			cosClient.setServiceResourceId(serviceResourceId);
			cosClient.setLocation(location);

			List<String> keyLst = cosClient.listObjects(bucketName, numOfRecords);
			logger.info("Number of Objects:" + keyLst.size());

			for (String key : keyLst) {
				YFCDocument objDoc = YFCDocument.createDocument("COSObject");
				YFCElement objEl = objDoc.getDocumentElement();
				objEl.setAttributes(rootEl.getAttributes());
				objEl.removeAttribute("Action");
				objEl.setAttribute(OBJECT_KEY, key);
				jobList.add(objDoc.getDocument());
			}
			logger.info("Number of Jobs:" + jobList.size());
			return jobList;
		} finally {
			cosClient.shutDown();
			logger.endTimer("getJobs");
		}

	}

	@Override
	public void executeJob(YFSEnvironment env, Document inDoc) throws Exception {
		logger.beginTimer("executeJob");
		if (YFCLogUtil.isDebugEnabled()) {
			logger.debug("Input message:" + YFCDocument.getDocumentFor(inDoc).getString());
		}
		COSClient cosClient = new COSClientImpl();
		try {
			
			YFCDocument objDoc = YFCDocument.getDocumentFor(inDoc);

			YFCElement rootEl = objDoc.getDocumentElement();

			String propApiKeyName = rootEl.getAttribute(API_KEY_NAME);
			String apiKey = YFSSystem.getProperties().getProperty(propApiKeyName);
			String endpointUrl = rootEl.getAttribute(ENDPOINT_URL);
			String serviceResourceId = rootEl.getAttribute(SERVICE_RESOURCE_ID);
			String bucketName = rootEl.getAttribute(INPUT_BUCKET_NAME);
			String location = rootEl.getAttribute(LOCATION);
			String keyName = rootEl.getAttribute(OBJECT_KEY);
			String processedBucketName = rootEl.getAttribute(SUCCESS_BUCKET_NAME);

			cosClient.setApiKey(apiKey);
			cosClient.setEndpointURL(endpointUrl);
			cosClient.setServiceResourceId(serviceResourceId);
			cosClient.setLocation(location);

			InputStream is = cosClient.readObject(bucketName, keyName);

			ByteArrayOutputStream result = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			int length;
			while ((length = is.read(buffer)) != -1) {
				result.write(buffer, 0, length);
			}

			logger.info("Object read:" + result.toString(StandardCharsets.UTF_8.name()));
			cosClient.moveObject(bucketName, keyName, processedBucketName, keyName);

			logger.info("Moved to " + processedBucketName + " Successfully..");

		} finally {
			cosClient.shutDown();
			logger.endTimer("executeJob");
		}
	}

}
