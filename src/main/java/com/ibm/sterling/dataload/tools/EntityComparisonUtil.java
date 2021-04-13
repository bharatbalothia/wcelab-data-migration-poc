package com.ibm.sterling.dataload.tools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.ibm.sterling.dataload.util.XMLUtil;

/**
 * 
 * This util-class takes two sets of entity-xmls from different Sterling
 * versions as inputs and compares the data-type of each column. Entity-xmls can
 * be obtained using one of these options -
 * <your_sterling_foundation>/repository/entity or
 * <exploded_resources_jar>/database/entity
 * 
 * The util makes an assumption that the data-type itself is same between two
 * versions. Example : "Text-20" implies NVARCHAR(20) in both old and new
 * versions. If needed, datatypes.xml from old and new versions should be
 * compared manually.
 * 
 */

public class EntityComparisonUtil {

	private static Logger log = Logger.getLogger(EntityComparisonUtil.class);
	private static String currentDir;

	// a crude map to store Table-names and column-details
	private static Map<String, DBTblColumn> entityComparisonMap = new HashMap<String, DBTblColumn>();

	public static void main(String[] args) throws Exception {

		if (args.length < 3) {
			printUsage();
			return;
		}

		String oldEntityDir = args[0];
		String newEntityDir = args[1];
		String reportDir = args[2];

		doCompare(oldEntityDir, newEntityDir, reportDir);
	}

	public static void doCompare(String oldEntityDir, String newEntityDir, String reportDir) throws Exception {

		currentDir = "old";
		readDirectory(new File(oldEntityDir));

		currentDir = "new";
		readDirectory(new File(newEntityDir));

		System.out.println(entityComparisonMap.size());

		writeMapToFile(reportDir);

	}

	// read all files in an entity directory and pick all table-column definitions
	public static void readDirectory(File dir) throws Exception {

		try {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (file.isDirectory()) {

					// ignore extensions and upgradeextensions folders
					if (!file.getName().contains("extensions")) {
						readDirectory(file);
					}

				} else {

					// consider only .xml files and ignore files like .sample
					if ("xml".equals(FilenameUtils.getExtension(file.getCanonicalPath()))) {
						scanEntities(file);
					}

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// a crude approach to track column data-types
	static class DBTblColumn {

		String tbl;
		String col;
		String oldDataType;
		String newDataType;

		DBTblColumn(String t, String c) {
			oldDataType = "column_not_found_in_old_version";
			newDataType = "column_not_found_in_new_version";
			tbl = t;
			col = c;
		}

		public boolean doDataTypesMatch() {
			return oldDataType.equals(newDataType);
		}

		public String toString() {
			return "\"" + tbl + "\"" + "," + "\"" + col + "\"" + "," + "\"" + oldDataType + "\"" + "," + "\""
					+ newDataType + "\"" + "," + doDataTypesMatch();
		}

	}

	public static void scanEntities(File file) {
		try {
			Document entities = XMLUtil.getDocument(file.getCanonicalPath());
			NodeList nlEntities = entities.getDocumentElement().getElementsByTagName("Entity");
			for (int i = 0; i < nlEntities.getLength(); i++) {

				Element entity = (Element) nlEntities.item(i);
				String tblName = entity.getAttribute("TableName");

				// ignore view and extn-tables if any
				if ("VIEW".equalsIgnoreCase(entity.getAttribute("EntityType"))) {
					log.info("Ignoring entity-definition for view : " + tblName);
					continue;
				}

				// ignore extn-tables if any
				if (!StringUtils.isBlank(tblName) && tblName.toUpperCase().startsWith("EXTN")) {
					log.info("Ignoring entity-definition for custom table : " + tblName);
					continue;
				}

				if (!StringUtils.isBlank(tblName) && !tblName.toUpperCase().startsWith("EXTN")) {

					NodeList columns = entity.getElementsByTagName("Attribute");
					for (int j = 0; j < columns.getLength(); j++) {

						Element eleColumn = (Element) columns.item(j);
						if (eleColumn.hasAttribute("ColumnName") && eleColumn.hasAttribute("DataType")) {
							String colName = eleColumn.getAttribute("ColumnName");
							String dataType = eleColumn.getAttribute("DataType");

							// ignore extn-columns if any
							if (colName.toUpperCase().startsWith("EXTN")) {
								log.info("Ignoring column : " + colName);
								continue;
							}

							String key = tblName + "," + colName;

							DBTblColumn tbCol = entityComparisonMap.get(key);

							if (tbCol == null) {
								tbCol = new DBTblColumn(tblName, colName);
							}

							if ("old".equals(currentDir)) {
								tbCol.oldDataType = dataType;
							} else {
								tbCol.newDataType = dataType;
							}

							entityComparisonMap.put(key, tbCol);
						}

					}

				}
			}
		} catch (Exception e) {
			log.error("Error reading " + file.getName() + ", " + e.getMessage());
		}
	}

	public static void writeMapToFile(String reportDir) throws Exception {

		// sort by table and column-name
		List<String> sortedKeys = new ArrayList<String>(entityComparisonMap.keySet());
		Collections.sort(sortedKeys);

		File file = new File(reportDir + "/EntityComparisonReport_" + System.currentTimeMillis() + ".csv");
		FileWriter writer = new FileWriter(file);
		writer.append("\"TABLE\",\"COLUMN\",\"OLD_DATA_TYPE\",\"NEW_DATA_TYPE\",\"IS_MATCH\"");
		writer.append("\n");

		for (String key : sortedKeys) {
			writer.append(entityComparisonMap.get(key).toString());
			writer.append("\n");
		}
		writer.flush();
		writer.close();

	}

	public static void printUsage() {

		log.info("This program expects these arguments - start");
		log.info("\t Arg 1: Path to directory containing Sterling (old version) entity-XMLs");
		log.info("\t Arg 2: Path to directory containing Sterling (new version) entity-XMLs");
		log.info("\t Arg 3: Path to directory where report should be printed");
		log.info("This program expects these arguments - end");

	}

}
