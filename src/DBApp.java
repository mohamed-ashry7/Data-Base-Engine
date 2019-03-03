import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

public class DBApp {
	String currentDir = System.getProperty("user.dir");

	////////////////////////////////////////// CREATE
	////////////////////////////////////////// ////////////////////////////////////////////////
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		String tableName = strTableName;
		new File(currentDir + "\\" + tableName).mkdirs();

		Set keySet = htblColNameType.keySet();

		Set TypeSet = new HashSet(htblColNameType.values());
		validateEnteredTypes(TypeSet);
		// changed the file OF DATA to be arraylist saved in .ser file to make
		// sure no one can edit it
		File metaData = new File(currentDir + "\\" + tableName + "\\metadata.csv");
		ArrayList<String> DATA = new ArrayList<>();
		ArrayList<Integer> PAGES = new ArrayList<>();

		try {
			DATA.add("PAGES: " + 0);
			DATA.add("Rows: " + 0);
			DATA.add("ClusteringTable: " + strClusteringKeyColumn);
			serializingAnObject(DATA, currentDir + "\\" + tableName + "\\DATA.ser");
			serializingAnObject(PAGES, currentDir + "\\" + tableName + "\\PAGES.ser");

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

	private void validateTypesOfValues(Hashtable<String, Object> h, String strTableName) throws DBAppException {

		File tableName = new File(currentDir + "\\" + strTableName);
		if (tableName == null) {
			throw new DBAppException("THIS TABLE DOES NOT EXIST ");
		}

		Set key = h.keySet();
		Iterator<String> it = key.iterator();
		ArrayList<String> newArr = new ArrayList<>();
		while (it.hasNext()) {
			newArr.add(it.next());
		}
		File f = new File(currentDir + "\\" + strTableName + "\\metadata.csv");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(f));
			br.readLine();

			String x = null;

			while ((x = br.readLine()) != null) {
				String[] strArray = x.split(",");

				for (int j = 0; j < newArr.size(); j++) {
					if (newArr.get(j).equals(strArray[1])) {
						if (!h.get(newArr.get(j)).getClass().getName().equals(strArray[2])) {
							newArr.remove(j);

							throw new DBAppException("THE TYPES ARE NOT CONSISTENT");
						} else {
							newArr.remove(j);
							break;
						}
					}
				}

			}
			if (newArr.size() != 0) {
				throw new DBAppException("THERE IS INVALID FIELD NAME ");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// catch (IOException e ) {
		//
		// }
	}

	private void validateEnteredTypes(Set<String> createdTypes) throws DBAppException {
		ArrayList<String> validTypes = new ArrayList<>();
		validTypes.add("java.lang.Integer");
		validTypes.add("java.lang.Double");
		validTypes.add("java.lang.String");
		validTypes.add("java.lang.Boolean");
		validTypes.add("java.util.Date");

		Iterator<String> it = createdTypes.iterator();
		while (it.hasNext()) {

			if (!validTypes.contains(it.next())) {
				throw new DBAppException("the types you entered are not valid");
			}
		}
	}

	private int numberOfCreatedPages(String strTableName) {

		ArrayList<String> DATA = (ArrayList<String>) deserializingAnObject(
				currentDir + "\\" + strTableName + "\\DATA.ser");
		String PAGES =(String) DATA.get(0);
		StringTokenizer str = new StringTokenizer(PAGES);
		str.nextToken();
		int pages = Integer.parseInt(str.nextToken());
		System.out.println();
		return pages;

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
		ArrayList<String> DATA = (ArrayList<String>) deserializingAnObject(
				currentDir + "\\" + strTableName + "\\DATA.ser");
		String theColumn = DATA.get(2);
		StringTokenizer str = new StringTokenizer(theColumn);
		str.nextToken() ;
		String theClusteringColumn = str.nextToken();

		return theClusteringColumn;
	}

	private void changeNoPages(String strTableName, String sign, int modifiedPage) {

		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\" + strTableName + "\\PAGES.ser");
		ArrayList<String> DATA = (ArrayList<String>) deserializingAnObject(
				currentDir + "\\" + strTableName + "\\DATA.ser");
		StringTokenizer str = new StringTokenizer(DATA.get(0));
		str.nextToken();
		int pages = Integer.parseInt(str.nextToken());
		int numberOFPages = PAGES.size();

		if (sign.equals("+")) {
			PAGES.add(new Integer(modifiedPage));
			pages++;
			DATA.set(0, "PAGES: " + pages) ; 
			serializingAnObject(DATA, currentDir + "\\" + strTableName + "\\DATA.ser");

		} else {
			PAGES.remove(new Integer(modifiedPage));
		}
		serializingAnObject(PAGES, currentDir + "\\" + strTableName + "\\PAGES.ser");

	}

	private void changeNoRows(String strTableName, String sign, int numberOfValues) {

		ArrayList<String> DATA = (ArrayList<String>) deserializingAnObject(
				currentDir + "\\" + strTableName + "\\DATA.ser");
		String RECORDS = DATA.get(1);
		StringTokenizer str = new StringTokenizer(RECORDS);
		str.nextToken();
		int records = Integer.parseInt(str.nextToken());
		if (sign.equals("+"))
			records++;
		else
			records -= numberOfValues;
		DATA.set(1, "ROWS: " + records);
		serializingAnObject(DATA, currentDir + "\\" + strTableName + "\\DATA.ser");

	}

	public void serializingAnObject(Object newObject, String pathName) {
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

	public Object deserializingAnObject(String Path) {
		Object o = null;
		try {
			FileInputStream fileIn = new FileInputStream(Path);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			o = in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException cc) {
			cc.printStackTrace();
		}
		return o;
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

	private Page whichPage(Hashtable<String, Object> record, String clustered, String Path) {
		Page lastPage = null;
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(Path + "\\PAGES.ser");
		Object value = record.get(clustered);

		for (int i = 0; i < PAGES.size(); i++) {

			String path = Path + "\\" + "Page" + PAGES.get(i).intValue() + ".ser";
			Page deserializedPage = (Page) deserializingAnObject(path);
			if (deserializedPage != null) {
				if (deserializedPage.isEmpty()){
					System.out.println(3);

					return deserializedPage ; 
				}
				Hashtable<String, Object> lastRecord = deserializedPage.getLastElement();
				Object lastValue = lastRecord.get(clustered);
				boolean flag = comparingValues(value, lastValue, "LESS");
				lastPage = deserializedPage;
				if (flag) {
					System.out.println(2);

					return deserializedPage;
				}
			}

		}
		System.out.println(1);

		return lastPage; // this line will be reached when we add the largest
							// clustering key value, we should return the last
							// page
	}

	private Page lastPage(String Path) {
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(Path + "\\PAGES.ser");

		Page p = (Page) deserializingAnObject(Path + "\\Page" + PAGES.get(PAGES.size() - 1).intValue() + ".ser");
		return p;
	}

	private Page createPage(String strTableName, Hashtable<String, Object> htblColNameValue) {
		int numberOfCreatedPages = numberOfCreatedPages(strTableName);
		String clusteringColumn = clusteringColumn(strTableName);
		String clusteringColumnType = htblColNameValue.get(clusteringColumn).getClass().getName();
		numberOfCreatedPages++;
		Page e = new Page(strTableName, numberOfCreatedPages);
		e.setClustering(clusteringColumn, clusteringColumnType);
		changeNoPages(strTableName, "+", numberOfCreatedPages);
		serializingAnObject(e, currentDir + "\\" + strTableName + "\\" + e.getPageName() + ".ser");
		return e;
	}

	///////////////////////////////// INSERT
	/////////////////////////////////////////////////////////////////////////////////
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Hashtable<String, Object> value = mapHash(htblColNameValue);
		validateTypesOfValues(htblColNameValue, strTableName);
		value.put("TouchDate", new Date());
		int numberOfCreatedPages = numberOfCreatedPages(strTableName);
		String clusteringColumn = clusteringColumn(strTableName);
		changeNoRows(strTableName, "+", 1);
		if (numberOfCreatedPages == 0) {
			createPage(strTableName, htblColNameValue);
		}
		Hashtable<String, Object> lastValue = null;
		Page lastPage = lastPage(currentDir + "\\" + strTableName);
		while (true) {
			Page toAddIn = whichPage(value, clusteringColumn, currentDir + "\\" + strTableName);
			if (toAddIn.isFull() && toAddIn.equals(lastPage)) {
				Page e = createPage(strTableName, htblColNameValue);
				lastValue = lastPage.removeLastElement();
				lastPage.addElement(value);
				e.addElement(lastValue);
				serializingAnObject(lastPage, currentDir + "\\" + strTableName + "\\" + lastPage.getPageName() + ".ser");
				serializingAnObject(e, currentDir + "\\" + strTableName + "\\" + e.getPageName() + ".ser");

				break;
			} else if (toAddIn.isFull()) {
				lastValue = toAddIn.removeLastElement();
				toAddIn.addElement(value);
				serializingAnObject(toAddIn, currentDir + "\\" + strTableName + "\\" + toAddIn.getPageName() + ".ser");
				value = lastValue;
			} else {
				toAddIn.addElement(value);
				serializingAnObject(toAddIn, currentDir + "\\" + strTableName + "\\" + toAddIn.getPageName() + ".ser");
				break;
			}

		}

	}

	///////////////////////////// UPDATE
	////////////////////////////////////////////////////////////////////////////
	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		validateTypesOfValues(htblColNameValue, strTableName);

		String clusteringValue = strKey;
		Hashtable<String, Object> value = mapHash(htblColNameValue);
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			if (e != null) {
				e.updateRecord(clusteringValue, htblColNameValue);
				serializingAnObject(e, currentDir + "\\" + strTableName + "\\" + e.getPageName() + ".ser");

			}
		}

	}

	////////////////////////////////////// DELETE//////////////////////////////////////////////////////////////////
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		validateTypesOfValues(htblColNameValue, strTableName);

		String clusteringColumn = clusteringColumn(strTableName);
		Hashtable<String, Object> value = mapHash(htblColNameValue);
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			if (e != null) {
				int numberOfDeletions = e.removeRecord(value);
				changeNoRows(strTableName, "-", numberOfDeletions);
				if (e.isEmpty()) {
					File f = new File(currentDir + "\\" + strTableName + "\\" + "Page" + i + ".ser");
					f.delete();
					changeNoPages(strTableName, "-", PAGES.get(i).intValue());
				} else {
					serializingAnObject(e, currentDir + "\\" + strTableName + "\\" + e.getPageName() + ".ser");
				}
			}
		}

	}

}
