package com.ibm.sterling.dataload.integration;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;

import com.ibm.sterling.dataload.util.XMLUtil;
import com.yantra.interop.util.YFSContextManager;
import com.yantra.shared.ycp.YFSContext;
import com.yantra.yfc.dblayer.YFCEntity;
import com.yantra.yfc.dblayer.YFCEntityDBHome;
import com.yantra.yfc.dblayer.YFCEntityDBHome.Attribute;
import com.yantra.yfc.dom.YFCDocument;
import com.yantra.yfc.log.YFCLogCategory;
import com.yantra.yfs.japi.YFSEnvironment;

public class ProcessData {

	private static YFCLogCategory log = YFCLogCategory.instance(ProcessData.class);

	/**
	 * This method can be called from a Sterling service, it can take a XML document
	 * with a list of elements and insert them into the corresponding OMS table.
	 * 
	 * <b> Sample input-doc : start </b>
	 * 
	 * <YFS_LINE_CHARGESList TableName='YFS_LINE_CHARGES' DBEntityClassName=
	 * 'YFS_Line_Charges'>
	 * <YFS_LINE_CHARGES CHARGEAMOUNT="3" CHARGEPERLINE="0" CHARGEPERUNIT="1"
	 * CHARGE_CATEGORY="Discount" CHARGE_NAME=" " CREATEPROGID= "OrderIntegServer"
	 * CREATETS="2020-04-08 09:52:07" CREATEUSERID= "OrderIntegServer"
	 * DBEntityClassName="YFS_Line_Charges" EXTN_SAMPLE_CLOB="" EXTN_VENDOR_ID=""
	 * HEADER_KEY="2020040809520714145736 " INVOICED_CHARGE_PER_LINE="0"
	 * INVOICED_EXTENDED_CHARGE="0" IS_MANUAL="Y" LINE_CHARGES_KEY=
	 * "202004080903520714145808" LINE_KEY="2020040809520714145756 " LOCKID="0"
	 * MODIFYPROGID="OrderIntegServer" MODIFYTS="2020-04-08 09:52:07" MODIFYUSERID=
	 * "OrderIntegServer" ORIGINAL_CHARGEPERLINE="0" ORIGINAL_CHARGEPERUNIT="0"
	 * RECORD_TYPE="ORD" REFERENCE=" " TableName= "YFS_LINE_CHARGES"/>
	 * <YFS_LINE_CHARGES CHARGEAMOUNT="2" CHARGEPERLINE="0" CHARGEPERUNIT="1"
	 * CHARGE_CATEGORY="Discount" CHARGE_NAME=" " CREATEPROGID="OrderIntegServer"
	 * CREATETS="2020-04-08 09:52:00" CREATEUSERID="OrderIntegServer"
	 * DBEntityClassName="YFS_Line_Charges" EXTN_SAMPLE_CLOB="" EXTN_VENDOR_ID=""
	 * HEADER_KEY="2020040809520114135230 " INVOICED_CHARGE_PER_LINE="0"
	 * INVOICED_EXTENDED_CHARGE="0" IS_MANUAL="Y" LINE_CHARGES_KEY=
	 * "202004080946520114135291" LINE_KEY="2020040809520114135244 " LOCKID="0"
	 * MODIFYPROGID="OrderIntegServer" MODIFYTS="2020-04-08 09:52:00" MODIFYUSERID=
	 * "OrderIntegServer" ORIGINAL_CHARGEPERLINE="0" ORIGINAL_CHARGEPERUNIT="0"
	 * RECORD_TYPE="ORD" REFERENCE=" " TableName="YFS_LINE_CHARGES"/>
	 * </YFS_LINE_CHARGESList>
	 * 
	 * <b> Sample input-doc : end </b>
	 * 
	 * @param env
	 * @param inDoc
	 * @return
	 * @throws Exception
	 */

	public Document processOMSEntity(YFSEnvironment env, Document inDoc) throws Exception {

		// TODO exception-handling
		// TODO generate insert-summary document and return from this method
		log.beginTimer("processOMSEntity");

		YFSContext ctx = YFSContextManager.getInstance().getContextFor(env);

		Element inEle = inDoc.getDocumentElement();
		String tableName = inEle.getAttribute("TableName");
		String sEntity = inEle.getAttribute("DBEntityClassName");

		log.info("Processing data-input document for table : " + tableName);

		Class<?> aDBHome = null;
		Class<?> aEntity = null;
		Method getInstance = null;
		Constructor<?> entityConstructor = null;

		YFCEntityDBHome<?, ?> oEntityDBHome = null;
		YFCEntity<?> oEntity = null;

		aDBHome = Class.forName("com.yantra.shared.dbclasses" + "." + sEntity + "DBHome");
		getInstance = aDBHome.getMethod("getInstance", new Class[0]);
		oEntityDBHome = (YFCEntityDBHome<?, ?>) getInstance.invoke(aDBHome, new Object[0]);

		aEntity = Class.forName("com.yantra.shared.dbclasses" + "." + sEntity + "Base");

		String rootElementName = oEntityDBHome.getXMLName();

		NodeList recordsForInsert = inEle.getElementsByTagName(tableName);
		int nlLen = recordsForInsert.getLength();

		for (int i = 0; i < nlLen; i++) {
			Document entityInputDoc = XMLUtil.createDocument(rootElementName);
			Element entityInputEle = entityInputDoc.getDocumentElement();

			entityConstructor = aEntity.getConstructor(new Class[0]);
			oEntity = (YFCEntity<?>) entityConstructor.newInstance();

			NamedNodeMap inputAttributes = ((Element) recordsForInsert.item(i)).getAttributes();
			int attributesLen = inputAttributes.getLength();

			for (int j = 0; j < attributesLen; j++) {
				Attr attribute = (Attr) inputAttributes.item(j);
				String attrName = attribute.getName();

				// TODO : This was added to ignore data-load specific attributes. Improve this
				// condition
				Attribute attrForColumnName = oEntityDBHome.getAttributeForColumnName(attrName);
				if (attrForColumnName == null) {
					continue;
				}

				String targetAttrName = attrForColumnName.getName();
				String targetXMLGroup = oEntity.getXMLGroup(targetAttrName);

				// TODO : Improve this condition
				if (targetAttrName.startsWith("Extn_")) {
					targetAttrName = targetAttrName.substring(5);
				}

				if (StringUtils.isEmpty(targetXMLGroup) || rootElementName.equals(targetXMLGroup)) {
					entityInputEle.setAttribute(targetAttrName, attribute.getValue());
				} else {

					Element childEle = (Element) entityInputEle.getElementsByTagName(targetXMLGroup).item(0);
					if (childEle == null) {
						childEle = entityInputDoc.createElement(targetXMLGroup);
						entityInputEle.appendChild(childEle);
					}

					childEle.setAttribute(targetAttrName, attribute.getValue());
				}

			}

			oEntity.basicReadXML(YFCDocument.getDocumentFor(entityInputDoc).getDocumentElement());
			YFCEntityDBHome.setDisableExtensions(true);
			oEntityDBHome.insert(ctx, oEntity);
		}

		// YFCLogger is not included in Maven project yet
		log.info("Inserted " + nlLen + " records in table : " + tableName);

		log.endTimer("processOMSEntity");

		return XMLUtil.createDocument("Success");
	}

}
