package com.ibm.sterling.dataload.tools;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.ibm.sterling.dataload.util.XMLUtil;

public class DataExtractCSVHandler {

	private static Logger log = Logger.getLogger(DataExtractCSVHandler.class);

//	public static String DATA_DIR_PATH = "C:/omsbase/data_files/10K/";
	
	// TODO : These constants should be read from a properties file 
	public static String CLIENT_LABEL = "DataExtract";
	public static int batchSize = 250;

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			printUsage();
			return;
		}

		String dataFilesDir = args[0];

		File data_dir = new File(dataFilesDir);
		if (data_dir.isDirectory()) {
			File[] files = data_dir.listFiles();
			for (int i = 0; i < files.length; i++) {

				if (files[i].isFile()) {
					String fileName = files[i].getName();
					if (fileName.contains(CLIENT_LABEL)) {
						String tableName = fileName.substring(0, fileName.indexOf("_" + CLIENT_LABEL));
						log.info("Processing data-extract file for table : " + tableName);
						readFileAndProcessRows(files[i], tableName);
					}
				}
			}
		}

	}

	public static void readFileAndProcessRows(File file, String tableName) throws Exception {

		String dbEntityClassName = formatTableName(tableName);

		String fileName = file.getCanonicalPath();
		Reader fileReader = new FileReader(file);
		CSVParser parser = CSVParser.parse(fileReader, setFormat());
		int expectedColumnCount = parser.getHeaderNames().size();
		log.info(fileName + " : Each row is expected to contain " + expectedColumnCount + " columns.");

		int lineCount = 0;

		String rootElement = tableName + "List";
		String listInputOpenTag = "<" + rootElement + " TableName='" + tableName + "' DBEntityClassName='"
				+ dbEntityClassName + "'>";
		String listInputCloseTag = "</" + rootElement + ">";

		StringBuffer elements = new StringBuffer();

		for (CSVRecord record : parser) {

			if (!record.isConsistent()) {
				log.info("Column-count mismtach for row below : Expected " + expectedColumnCount + ", Actual "
						+ record.size() + ".");
				log.info(record.toString());
				continue;
			}

			Document tableRowAsADoc = XMLUtil.createDocument(tableName);
			Element tableRowAsAEle = tableRowAsADoc.getDocumentElement();
			tableRowAsAEle.setAttribute("TableName", tableName);
			tableRowAsAEle.setAttribute("DBEntityClassName", dbEntityClassName);

			Map<String, String> recordMap = record.toMap();

			for (Map.Entry<String, String> entry : recordMap.entrySet()) {
				tableRowAsAEle.setAttribute(entry.getKey(), entry.getValue());
			}

			elements.append(XMLUtil.getXMLString(tableRowAsADoc));

			if (record.getRecordNumber() % batchSize == 0) {
				String message = listInputOpenTag + elements.toString() + listInputCloseTag;
				MessageSender.send("tcp://localhost:61616", message, "DATA_LOAD");
				elements.setLength(0);
			}

		}

		if (!StringUtils.isEmpty(elements)) {
			String message = listInputOpenTag + elements + listInputCloseTag;
			MessageSender.send("tcp://localhost:61616", message, "DATA_LOAD");
		}

		fileReader.close();
		log.info("Found " + lineCount + " valid rows in " + fileName);

	}

	public static CSVFormat setFormat() {

		return CSVFormat.DEFAULT.withFirstRecordAsHeader();

	}

	/**
	 * For a table name like YFS_ORDER_HEADER, this method returns YFS_Order_Header
	 * 
	 * @param tableName
	 * @return
	 */
	public static String formatTableName(String input) {

		String tName = WordUtils.capitalize(input.toLowerCase(), '_');
		String tPrefix = tName.substring(0, tName.indexOf('_'));
		tPrefix = tPrefix.toUpperCase();
		String output = tPrefix + tName.substring(tName.indexOf('_'));

		return output;

	}

	public static void printUsage() {

		log.info("This program expects these arguments - start");
		log.info("\t Arg 1: Path to directory containing extracted CSV files");
		log.info("This program expects these argument - end");

	}

}
