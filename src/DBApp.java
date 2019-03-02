import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

public class DBApp {
	String currentDir = System.getProperty("user.dir");
////////////////////////////////////////// CREATE ////////////////////////////////////////////////
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		String tableName = strTableName;
		new File(currentDir + "\\" + tableName).mkdirs();

		Set keySet = htblColNameType.keySet();
		
		Set TypeSet = new HashSet(htblColNameType.values());
		validateTypes(TypeSet);
		
		File metaData = new File(currentDir + "\\" + tableName + "\\metadata.csv");
		File DATA = new File(currentDir + "\\" + tableName + "\\DATA.txt");
		try {
			PrintWriter dataWriter = new PrintWriter(DATA.getPath());
			dataWriter.println("Page: " + 0);
			dataWriter.println("Rows: " + 0);
			dataWriter.println("ClusteringTable: " + strClusteringKeyColumn);
			dataWriter.flush();
			dataWriter.close();
			PrintWriter writer = new PrintWriter(metaData.getPath());
			String firstLine = "Table Name , Column Name , Column Type , Key , Indexed";
			writer.println(firstLine);
			Iterator<String> it = keySet.iterator();
			// writing in metaData File
			while (it.hasNext()) {
				String key = it.next();
				String k = "False";
				if (strClusteringKeyColumn.equals(key)) {
					k = "True";
				}
				String Line = tableName + "," + key + "," + (String) htblColNameType.get(key) + "," + k + ","
						+ "False ";
				writer.println(Line);
			}
			writer.flush();
			writer.close();
		} catch (FileNotFoundException e) {

		}

	}
	
	private void validateTypes(Set<Object> createdTypes) throws DBAppException {
		String validTypes[] = { "java.lang.Integer", "java.lang.Double", "java.lang.String", "java.lang.Boolean", "java.util.Date" };
		Set <String> validTypes2 = Set.of(validTypes);
		System.out.println(createdTypes);
		Iterator<Object> it = createdTypes.iterator();
		while (it.hasNext()) {
			String type = (String)it.next();
			String type2 = type.toLowerCase().trim();
			if(!validTypes2.contains(type2)){
				throw new DBAppException("the types you entered are not valid");
			}
		}
	}


	private boolean createOrNot(String strTableName) {
		try {
			File tableFile = new File(currentDir + "\\" + strTableName + "\\DATA.txt");
			BufferedReader br = new BufferedReader(new FileReader(tableFile));
			br.readLine();
//			int records = Integer.parseInt(br.readLine());	// this is equal to parseInt("Rows: 0") ,we need to parse out the first 5 chars OR use tokenizer like in lastPage method
			StringTokenizer str = new StringTokenizer(br.readLine());
			str.nextToken();
			int records = Integer.parseInt(str.nextToken());

//			return records % 200 == 0;	// records from 1 to 200 will return true, hence creating a page every time we insert a record
			return (records % 200 == 0) ;
		} catch (IOException e) {
			return false;
		}

		} catch (IOException e) {
			return false;
		}
	}

	private int lastPage(String strTableName) {
		try {
			File tableFile = new File(currentDir + "\\" + strTableName + "\\DATA.txt");
			BufferedReader br = new BufferedReader(new FileReader(tableFile));
			StringTokenizer str = new StringTokenizer(br.readLine());
			str.nextToken();
			int pages = Integer.parseInt(str.nextToken());
			return pages;

		} catch (IOException e) {
			return 0;
		}
	}

	private Hashtable<String, Object> mapHash(Hashtable<String, Object> htblColNameValue) {
		Hashtable<String, Object> value = new Hashtable<String, Object>();
		Set keySet = htblColNameValue.keySet();
		Iterator<String> it = keySet.iterator();
		while (it.hasNext()) {
			String key = it.next();
			value.put(key, htblColNameValue.get(key));
		}
		return value;
	}

	private String clusteringColumn(String strTableName) {
		try {
			File tableFile = new File(currentDir + "\\" + strTableName + "\\DATA.txt");
			BufferedReader br = new BufferedReader(new FileReader(tableFile));
			br.readLine();
			br.readLine();
			StringTokenizer str = new StringTokenizer(br.readLine());
			str.nextToken();
			return str.nextToken();

		} catch (IOException e) {
			return "";

		}
	}

	private void increaseNoPages(String strTableName) {
		try {
			File tableFile = new File(currentDir + "\\" + strTableName + "\\DATA.txt");
			BufferedReader br = new BufferedReader(new FileReader(tableFile));
			String line1 = br.readLine();
			String line2 = br.readLine();
			String line3 = br.readLine();

			StringTokenizer str = new StringTokenizer(line1);
			String one = str.nextToken();
			int pages = Integer.parseInt(str.nextToken()) + 1;
			PrintWriter writer = new PrintWriter(tableFile.getPath());
			line1 = one + " " + pages;
			writer.println(line1);
			writer.println(line2);
			writer.println(line3);
			writer.flush();
			writer.close();
		} catch (IOException e) {
		}
	}

	private void serializingAnObject(Object newObject, String pathName) {
		try {
			FileOutputStream fileOut = new FileOutputStream(pathName);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(newObject);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Page deserializingAnObject(String Path) {
		Page p = null;
		try {
			FileInputStream fileIn = new FileInputStream(Path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			p = (Page) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException cc) {
			cc.printStackTrace();
		}
		return p;
	}

	private boolean comparingValues(Object value, Object lastValue, String operation) {

		String cls = value.getClass().getName();

		switch (cls) {
		case "java.lang.Integer":
			if (((Integer) value).compareTo((Integer) lastValue) < 0 && operation.equals("LESS")
					|| ((Integer) value).compareTo((Integer) lastValue) == 0 && operation.equals("EQUAL")) {
				return true;
			} else {
				return false;
			}

		case "java.lang.Double":
			if (((Double) value).compareTo((Double) lastValue) < 0 && operation.equals("LESS")
					|| ((Double) value).compareTo((Double) lastValue) == 0 && operation.equals("EQUAL")) {
				return true;
			} else {
				return false;
			}

		case "java.lang.String":
			if (((String) value).compareTo((String) lastValue) < 0 && operation.equals("LESS")
					|| ((String) value).compareTo((String) lastValue) == 0 && operation.equals("EQUAL")) {
				return true;
			} else {
				return false;
			}

		case "java.lang.Boolean":
			if (((Boolean) value).compareTo((Boolean) lastValue) < 0 && operation.equals("LESS")
					|| ((Boolean) value).compareTo((Boolean) lastValue) == 0 && operation.equals("EQUAL")) {
				return true;
			} else {
				return false;
			}

		case "java.util.Date":
			if (((Date) value).compareTo((Date) lastValue) < 0 && operation.equals("LESS")
					|| ((Date) value).compareTo((Date) lastValue) == 0 && operation.equals("EQUAL")) {
				return true;
			} else {
				return false;
			}

		}
		return false;
	}

	private Page whichPage(Hashtable<String, Object> record, String clustered, String Path, int lastPage) {

		Object value = record.get(clustered);

		for (int i = 1; i <= lastPage; i++) {

			String path = Path + "\\" + "Page" + i + ".ser";
			Page deserializedPage = deserializingAnObject(path);
			if (deserializedPage != null) {
				Hashtable<String, Object> lastRecord = deserializedPage.getLastElement();
				Object lastValue = lastRecord.get(clustered);
				boolean flag = comparingValues(value, lastValue, "LESS");
				if (flag) {
					return deserializedPage;
				}
			}

		}

		return null;	// this line will be reached when we add the largest clustering key value, we should return the last page
	}
///////////////////////////////// INSERT //////////////////////////////////////////////////
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Hashtable<String, Object> value = mapHash(htblColNameValue);
		value.put("Date", new Date());
		int lastPage = lastPage(strTableName);
		String clusteringColumn = clusteringColumn(strTableName);
		String clusteringColumnType = htblColNameValue.get(clusteringColumn).getClass().getName();
		Page currentPage = null;
		if (lastPage == 0 || createOrNot(strTableName)) {
			lastPage++;
			Page e = new Page(strTableName, lastPage);
			e.setClustering(clusteringColumn, clusteringColumnType);
			increaseNoPages(strTableName);
			serializingAnObject(e, currentDir + "\\" + strTableName + "\\" + e.getPageName() + ".ser");
		}
		Hashtable<String, Object> lastValue = null;
		while (true) {
			Page toAddIn = whichPage(value, clusteringColumn, currentDir + "\\" + strTableName, lastPage);
			if (toAddIn.isFull()) {
				lastValue = toAddIn.removeLastElement();
				toAddIn.addElement(value);
				serializingAnObject(toAddIn, currentDir + "\\" + strTableName + "\\" + toAddIn.getPageName() + ".ser");
				value = lastValue;  //What now? should we call insertIntoTable(value) again ?
			} else {
				toAddIn.addElement(value);
				serializingAnObject(toAddIn, currentDir + "\\" + strTableName + "\\" + toAddIn.getPageName() + ".ser");
				break;
			}

		}

	}
///////////////////////////// UPDATE ///////////////////////////////////////////////
	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		
		
		

	}
//////////////////////////////////////DELETE//////////////////////////////////////////////////////////////////
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		int lastPage = lastPage(strTableName);
		String clusteringColumn = clusteringColumn(strTableName);
		Hashtable<String, Object> value = mapHash(htblColNameValue);

		for (int i = 1; i <= lastPage; i++) {
			Page e = deserializingAnObject(currentDir + "\\" + strTableName + "\\" + "Page" + i + ".ser");
			if (e != null) {
				e.removeRecord(value);
				if (e.isEmpty()) {
					File f = new File(currentDir + "\\" + strTableName + "\\" + "Page" + i + ".ser");
					f.delete();
				} else {
					serializingAnObject(e, currentDir + "\\" + strTableName + "\\" + e.getPageName() + ".ser");

				}
			}
		}

	}

}
