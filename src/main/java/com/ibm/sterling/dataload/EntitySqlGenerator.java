package com.ibm.sterling.dataload;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yantra.yfc.dblayer.YFCEntity;
import com.yantra.yfc.dblayer.YFCEntityDBHome;
import com.yantra.yfc.dblayer.YFCEntityDBHome.Attribute;

public class EntitySqlGenerator {

	YFCEntity<?> oEntity = null;
	YFCEntityDBHome<?, ?> dbHome;
	String dbType = null;

	public void process(String args, String dbType) throws Exception {
		String sEntity = args;

		setEntityClasses(sEntity);
		this.dbType = dbType;

		@SuppressWarnings("unchecked")
		Collection<Attribute> attrList = dbHome.getAttributes();

		List<Attribute> lst = new ArrayList<YFCEntityDBHome.Attribute>(attrList);

		Map<String, Map<String, String>> colMaps = loadNestedColumnMaps(lst);

		String entityRootName = dbHome.getXMLName();
		String rootTableName = dbHome.getTableName();
		Map<String, String> rootMap = colMaps.get(entityRootName);
		StringBuilder sb = new StringBuilder();

		sb.append("select xmlelement(");
		if ("DB2".contentEquals(dbType))
			sb.append(" name ");
		sb.append("\"").append(entityRootName).append("\", xmlattributes ('").append(args)
				.append("' as \"DBEntityClassName\",     ");
		addColumnsToBuffer(rootMap, sb);
		sb.append(")");

		if (colMaps.size() > 1) {
			addChildElements(colMaps, entityRootName, rootMap, sb);
		}
		sb.append(") from ").append(rootTableName).append(";");
		System.out.println("sql:" + sb);
		//System.out.println("\n\n\n\n\nColumns:\n");
		//printAllColumns(lst);
	}

	private void setEntityClasses(String sEntity) throws Exception {
		Class<?> aDBHome = null;
		Method getInstance = null;
		aDBHome = Class.forName("com.yantra.shared.dbclasses" + "." + sEntity + "DBHome");
		getInstance = aDBHome.getMethod("getInstance", new Class[0]);
		dbHome = (YFCEntityDBHome<?, ?>) getInstance.invoke(aDBHome, new Object[0]);

		Class<?> aEntity = null;
		aEntity = Class.forName("com.yantra.shared.dbclasses" + "." + sEntity + "Base");
		oEntity = (YFCEntity<?>) aEntity.getConstructor().newInstance();
	}

	private Map<String, Map<String, String>> loadNestedColumnMaps(List<Attribute> lst) {
		Map<String, Map<String, String>> colMaps = new HashMap<String, Map<String, String>>();

		for (Attribute attribute : lst) {
			if (attribute.isVirtual()) {
				continue;
			}
			String xmlGroup = oEntity.getXMLGroup(attribute.getName());
			Map<String, String> intMap = colMaps.get(xmlGroup);
			if (null == intMap) {
				intMap = new HashMap<String, String>();
				colMaps.put(xmlGroup, intMap);
				intMap.put(attribute.getName(), attribute.getColumnName());
			} else {
				intMap.put(attribute.getName(), attribute.getColumnName());
			}
			/*
			 * System.out.println(attribute.getIndex() + "," + attribute.getName() + "," +
			 * attribute.getColumnName() + "," + xmlGroup + "," + attribute.getDataType() +
			 * "," + attribute.isVirtual());
			 */
		}
		return colMaps;
	}

	public void printAllColumns(List<Attribute> lst) {
		Collections.sort(lst, new Comparator<YFCEntityDBHome.Attribute>() {

			@Override
			public int compare(YFCEntityDBHome.Attribute o1, YFCEntityDBHome.Attribute o2) {
				// TODO Auto-generated method stub
				return o1.getIndex() - o2.getIndex();
			}
		});
		for (Attribute attribute : lst) {
			String xmlGroup = oEntity.getXMLGroup(attribute.getName());
			System.out.println(attribute.getIndex() + "," + attribute.getName() + "," + attribute.getColumnName() + ","
					+ xmlGroup + "," + attribute.getDataType() + "," + attribute.isVirtual());

		}
	}

	private void addChildElements(Map<String, Map<String, String>> colMaps, String entityRootName,
			Map<String, String> rootMap, StringBuilder sb) {

		for (String elName : colMaps.keySet()) {
			if (entityRootName.contentEquals(elName) || "NOT_SHOWN".contentEquals(elName)) {
				continue;
			}

			sb.append(", ");
			rootMap = colMaps.get(elName);
			sb.append("xmlelement(");
			if ("DB2".contentEquals(dbType))
				sb.append("name ");
			sb.append("\"").append(elName).append("\", xmlattributes(");
			addColumnsToBuffer(rootMap, sb);
			sb.append("))");
		}
	}

	private void addColumnsToBuffer(Map<String, String> rootMap, StringBuilder sb) {
		boolean firstRec = true;
		List<String> keyLst = new ArrayList<String>(rootMap.keySet());
		Collections.sort(keyLst, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				Attribute a1 = EntitySqlGenerator.this.dbHome.getAttribute(o1);
				Attribute a2 = EntitySqlGenerator.this.dbHome.getAttribute(o2);
				return a1.getIndex() - a2.getIndex();
			}
		});
		for (String xmlName : keyLst) {
			String colName = rootMap.get(xmlName);
			if (firstRec) {
				firstRec = false;
			} else {
				sb.append(", ");
			}
			sb.append(colName).append(" as \"").append(xmlName).append("\"");
		}
	}

	public static void main(String[] args) throws Exception {
		EntitySqlGenerator loader = new EntitySqlGenerator();

		loader.process("YFS_Order_Line", "DB2");
	}
}
