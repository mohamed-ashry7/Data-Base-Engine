import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

public class DBApp implements Serializable {
	String currentDir = System.getProperty("user.dir");

	public void init() {

	}

	////////////////////////////////////////// CREATE
	////////////////////////////////////////////////////////////////////////////////////////
	public void checkIfExists(String strTableName) throws DBAppException {
		File file = new File(currentDir + "\\data");
		String[] directories = file.list(new FilenameFilter() {
			@Override
			public boolean accept(File current, String name) {
				return new File(current, name).isDirectory();
			}
		});
		for (int i = 0; i < directories.length; i++) {
			if (directories[i].equals(strTableName)) {
				throw new DBAppException("SORRY THE TABLE NAME ALREADY EXISTS");
			}
		}
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		// TODO change the

		// checkIfExists(strTableName);

		String tableName = strTableName;
		new File(currentDir + "\\data\\" + tableName).mkdirs();

		Set keySet = htblColNameType.keySet();

		Set TypeSet = new HashSet(htblColNameType.values());
		validateEnteredTypes(TypeSet);
		// changed the file OF DATA to be arraylist saved in .ser file to make
		// sure no one can edit it
		File metaData = new File(currentDir + "\\data\\" + tableName + "\\metadata.csv");
		ArrayList<String> DATA = new ArrayList<>();
		ArrayList<Integer> PAGES = new ArrayList<>();

		try {
			DATA.add("PAGES: " + 0);
			DATA.add("Rows: " + 0);
			DATA.add("ClusteringTable: " + strClusteringKeyColumn);
			serializingAnObject(DATA, currentDir + "\\data\\" + tableName + "\\DATA.ser");
			serializingAnObject(PAGES, currentDir + "\\data\\" + tableName + "\\PAGES.ser");

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

		File tableName = new File(currentDir + "\\data\\" + strTableName);
		if (tableName == null) {
			throw new DBAppException("THIS TABLE DOES NOT EXIST ");
		}

		Set key = h.keySet();
		Iterator<String> it = key.iterator();
		ArrayList<String> newArr = new ArrayList<>();
		while (it.hasNext()) {
			newArr.add(it.next());
		}
		File f = new File(currentDir + "\\data\\" + strTableName + "\\metadata.csv");
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
			if (newArr.size() != 0 && !newArr.get(0).equals("TouchDate")) {
				throw new DBAppException("THERE IS INVALID FIELD NAME ");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

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
				currentDir + "\\data\\" + strTableName + "\\DATA.ser");
		String PAGES = (String) DATA.get(0);
		StringTokenizer str = new StringTokenizer(PAGES);
		str.nextToken();
		int pages = Integer.parseInt(str.nextToken());
		// System.out.println();
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
				currentDir + "\\data\\" + strTableName + "\\DATA.ser");
		String theColumn = DATA.get(2);
		StringTokenizer str = new StringTokenizer(theColumn);
		str.nextToken();
		String theClusteringColumn = str.nextToken();

		return theClusteringColumn;
	}

	private void changeNoPages(String strTableName, String sign, int modifiedPage) {

		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		ArrayList<String> DATA = (ArrayList<String>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\DATA.ser");
		StringTokenizer str = new StringTokenizer(DATA.get(0));
		str.nextToken();
		int pages = Integer.parseInt(str.nextToken());
		int numberOFPages = PAGES.size();

		if (sign.equals("+")) {
			PAGES.add(new Integer(modifiedPage));
			pages++;
			DATA.set(0, "PAGES: " + pages);
			serializingAnObject(DATA, currentDir + "\\data\\" + strTableName + "\\DATA.ser");

		} else {
			PAGES.remove(new Integer(modifiedPage));
		}
		serializingAnObject(PAGES, currentDir + "\\data\\" + strTableName + "\\PAGES.ser");

	}

	private void changeNoRows(String strTableName, String sign, int numberOfValues) {

		ArrayList<String> DATA = (ArrayList<String>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\DATA.ser");
		String RECORDS = DATA.get(1);
		StringTokenizer str = new StringTokenizer(RECORDS);
		str.nextToken();
		int records = Integer.parseInt(str.nextToken());
		if (sign.equals("+"))
			records++;
		else
			records -= numberOfValues;
		DATA.set(1, "ROWS: " + records);
		serializingAnObject(DATA, currentDir + "\\data\\" + strTableName + "\\DATA.ser");

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

	public void showContent(String tableName) {
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + tableName + "\\PAGES.ser");

		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + tableName + "\\Page" + PAGES.get(i).intValue() + ".ser");
			e.printVector();
		}

	}

	public void showBitmap(String tableName, String colName) {

	}

	private Page whichPage(Hashtable<String, Object> record, String clustered, String Path) {
		Page lastPage = null;
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(Path + "\\PAGES.ser");
		Object value = record.get(clustered);

		for (int i = 0; i < PAGES.size(); i++) {

			String path = Path + "\\" + "Page" + PAGES.get(i).intValue() + ".ser";
			Page deserializedPage = (Page) deserializingAnObject(path);
			if (deserializedPage != null) {
				if (deserializedPage.isEmpty()) {
					// System.out.println(3);

					return deserializedPage;
				}
				Hashtable<String, Object> lastRecord = deserializedPage.getLastElement();
				Object lastValue = lastRecord.get(clustered);
				boolean flag = comparingValues(value, lastValue, "LESS");
				lastPage = deserializedPage;
				if (flag) {
					// System.out.println(2);

					return deserializedPage;
				}
			}

		}
		// System.out.println(1);

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
		Page e = new Page(strTableName, numberOfCreatedPages, this);
		e.setClustering(clusteringColumn, clusteringColumnType);
		changeNoPages(strTableName, "+", numberOfCreatedPages);
		// System.out.println(currentDir + "\\data\\" + strTableName + "\\" +
		// e.getPageName() + ".ser");
		serializingAnObject(e, currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");
		return e;
	}

	///////////////////////////////// INSERT
	/////////////////////////////////////////////////////////////////////////////////
	public ArrayList<String> getAllIndex(String strTableName, Hashtable<String, Object> htblColNameValue) {
		/*
		 * 
		 * 
		 * 
		 * 
		 * 
		 */
		ArrayList<String> indices = new ArrayList<>();
		try {
			BufferedReader br = new BufferedReader(
					new FileReader(new File(currentDir + "\\data\\" + strTableName + "\\metadata.csv")));
			br.readLine();
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] spl = line.split(",");
				if (spl[4].equals("TRUE")) {
					// System.out.println();
					// System.out.println(spl[1]);
					Set keys = htblColNameValue.keySet();
					Iterator<String> it = keys.iterator();
					while (it.hasNext()) {
						String x = it.next();
						if (x.equals(spl[1])) {
							indices.add(spl[1]);
						}
					}

				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		return indices;
	}

	public void checkDistinct(String strTableName, Hashtable<String, Object> htblColNameValue) {

		try {
			BufferedReader br = new BufferedReader(
					new FileReader(new File(currentDir + "\\data\\" + strTableName + "\\metadata.csv")));
			br.readLine();
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] spl = line.split(",");
				if (spl[4] == "TRUE") {
					ArrayList<Object> dis = getDistinctValues(strTableName, spl[1]);

					if (!dis.contains(htblColNameValue.get(spl[4]))) {
						insertNewBitmapObject(strTableName, htblColNameValue.get(spl[1]), spl[1]);

					}
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	public void updateTheIndexFromInsert(String strTableName, String pageName, ArrayList<String> names,
			Hashtable<String, Object> htblColNameValue, int position) {
		for (String name : names)
			insertValueBitmapIndex(strTableName, pageName, name, htblColNameValue.get(name), position);

	}

	// updateTheIndexFromDelete(strTableName, e.getPageName(), indexed,
	// htblColNameValue,
	// insta.getPos());
	public void updateTheIndexFromDelete(String strTableName, String pageName, ArrayList<String> names,
			Hashtable<String, Object> htblColNameValue, int position) {
		for (String name : names)
			deleteValueBitmapIndex(strTableName, pageName, name, htblColNameValue.get(name), position);

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		ArrayList<String> indecies = getAllIndex(strTableName, htblColNameValue);
		Hashtable<String, Object> value = mapHash(htblColNameValue);
		validateTypesOfValues(htblColNameValue, strTableName);
		checkDistinct(strTableName, htblColNameValue);
		value.put("TouchDate", new Date());
		int numberOfCreatedPages = numberOfCreatedPages(strTableName);
		String clusteringColumn = clusteringColumn(strTableName);
		changeNoRows(strTableName, "+", 1);
		if (numberOfCreatedPages == 0) {
			createPage(strTableName, htblColNameValue);
		}
		Hashtable<String, Object> lastValue = null;
		Page lastPage = lastPage(currentDir + "\\data\\" + strTableName);
		while (true) {
			Page toAddIn = whichPage(value, clusteringColumn, currentDir + "\\data\\" + strTableName);

			if (toAddIn.isFull() && toAddIn.getPageName().equals(lastPage.getPageName())) {

				Page e = createPage(strTableName, htblColNameValue);
				if (lastPage.getLastElement().get(clusteringColumn).toString()
						.compareTo(value.get(clusteringColumn).toString()) < 0) {

					int pos = e.addElement(value);
					updateTheIndexFromInsert(strTableName, e.getPageName(), indecies, htblColNameValue, pos);

				} else {
					lastValue = lastPage.removeLastElement();
					updateTheIndexFromDelete(strTableName, lastPage.getPageName(), indecies, htblColNameValue,
							lastPage.getNumberOfRows() - 1);
					int pos1 = lastPage.addElement(value);
					int pos2 = e.addElement(lastValue);
					updateTheIndexFromInsert(strTableName, lastPage.getPageName(), indecies, htblColNameValue, pos1);
					updateTheIndexFromInsert(strTableName, e.getPageName(), indecies, htblColNameValue, pos2);

				}
				serializingAnObject(lastPage,
						currentDir + "\\data\\" + strTableName + "\\" + lastPage.getPageName() + ".ser");
				serializingAnObject(e, currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");

				break;
			} else if (toAddIn.isFull()) {

				lastValue = toAddIn.removeLastElement();
				updateTheIndexFromDelete(strTableName, toAddIn.getPageName(), indecies, htblColNameValue,
						toAddIn.getNumberOfRows() - 1);

				int pos = toAddIn.addElement(value);
				updateTheIndexFromInsert(strTableName, toAddIn.getPageName(), indecies, htblColNameValue, pos);

				serializingAnObject(toAddIn,
						currentDir + "\\data\\" + strTableName + "\\" + toAddIn.getPageName() + ".ser");
				value = lastValue;
			} else {
				int pos = toAddIn.addElement(value);
				updateTheIndexFromInsert(strTableName, toAddIn.getPageName(), indecies, htblColNameValue, pos);

				serializingAnObject(toAddIn,
						currentDir + "\\data\\" + strTableName + "\\" + toAddIn.getPageName() + ".ser");
				break;
			}

		}

	}

	///////////////////////////// UPDATE
	////////////////////////////////////////////////////////////////////////////
	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		validateTypesOfValues(htblColNameValue, strTableName);
		Hashtable<String, Object> htblKEY = new Hashtable<>();
		htblKEY.put(strKey, new Object());
		ArrayList<String> indexedCol = getAllIndex(strTableName, htblKEY);
		String clusteringValue = strKey;
		Hashtable<String, Object> value = mapHash(htblColNameValue);
		if (indexedCol.size() == 0) {
			ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
			for (int i = 0; i < PAGES.size(); i++) {
				Page e = (Page) deserializingAnObject(
						currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
				if (e != null) {
					ArrayList<String> indexed = getAllIndex(strTableName, htblColNameValue);
					ArrayList<Page.Pair> PositionsAndPreviousValue = e.updateRecord(clusteringValue, htblColNameValue);
					serializingAnObject(e, currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");

					if (indexed != null) {
						Set keys = htblColNameValue.keySet();
						Iterator<String> it = keys.iterator();
						while (it.hasNext()) {
							String k = it.next();
							if (indexed.contains(k)) {
								for (int f = 0; f < PositionsAndPreviousValue.size(); f++) {

									boolean flag = deleteBitmapObject(strTableName,
											PositionsAndPreviousValue.get(f).getPreviousVal().get(k), k);
									if (flag) {
										insertNewBitmapObject(strTableName, htblColNameValue.get(k), k);
									}
								}
							}
						}
						for (Page.Pair insta : PositionsAndPreviousValue) {
							updateTheIndexFromDelete(strTableName, e.getPageName(), indexed, htblColNameValue,
									insta.getPos());
							updateTheIndexFromInsert(strTableName, e.getPageName(), indexed, htblColNameValue,
									insta.getPos());

						}

					}

				}
			}
		} else {

			File folder = new File(currentDir + "\\data\\" + strTableName);
			File[] listOfFiles = folder.listFiles();
			Bitmap b = null;
			String strColumn = clusteringColumn(strTableName);
			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].getName().contains(strColumn)) {
					b = (Bitmap) deserializingAnObject(listOfFiles[i].getPath());
					if (b.bitmapContainsKey(strColumn)) {
						Vector<Hashtable<Object, ArrayList<Triple>>> allElements = b.getAllBitmaps();
						for (int j = 0; j < allElements.size(); j++) {
							if (allElements.get(j).keys().nextElement().toString().equals(strColumn)) {
								ArrayList<Triple> triples = allElements.get(j).get(strColumn);
								for (int kk = 0; kk < triples.size(); kk++) {
									if (triples.get(kk).getValue() == 1) {

										Page e = (Page) deserializingAnObject(currentDir + "\\data\\" + strTableName
												+ "\\" + "Page" + triples.get(kk).getPageNumber() + ".ser");
										if (e != null) {
											ArrayList<String> indexed = getAllIndex(strTableName, htblColNameValue);
											ArrayList<Page.Pair> PositionsAndPreviousValue = e
													.updateRecord(clusteringValue, htblColNameValue);
											serializingAnObject(e, currentDir + "\\data\\" + strTableName + "\\"
													+ e.getPageName() + ".ser");

											if (indexed != null) {
												Set keys = htblColNameValue.keySet();
												Iterator<String> it = keys.iterator();
												while (it.hasNext()) {
													String k = it.next();
													if (indexed.contains(k)) {
														for (int f = 0; f < PositionsAndPreviousValue.size(); f++) {

															boolean flag = deleteBitmapObject(strTableName,
																	PositionsAndPreviousValue.get(f).getPreviousVal()
																			.get(k),
																	k);
															if (flag) {
																insertNewBitmapObject(strTableName,
																		htblColNameValue.get(k), k);
															}
														}
													}
												}
												for (Page.Pair insta : PositionsAndPreviousValue) {
													updateTheIndexFromDelete(strTableName, e.getPageName(), indexed,
															htblColNameValue, insta.getPos());
													updateTheIndexFromInsert(strTableName, e.getPageName(), indexed,
															htblColNameValue, insta.getPos());

												}

											}

										}
										break;
									}
								}
							}
						}

						break;
					}

				}
			}

		}
	}

	////////////////////////////////////// DELETE//////////////////////////////////////////////////////////////////
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		validateTypesOfValues(htblColNameValue, strTableName);

		ArrayList<String> indexedCol = getAllIndex(strTableName, htblColNameValue);
		String clusteringColumn = clusteringColumn(strTableName);
		Hashtable<String, Object> value = mapHash(htblColNameValue);
		if (indexedCol.size() == 0) {
			ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
			for (int i = 0; i < PAGES.size(); i++) {
				Page e = (Page) deserializingAnObject(
						currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
				if (e != null) {
					ArrayList<Integer> positions = e.removeRecord(value);
					ArrayList<String> indexedColumns = getAllIndex(strTableName, htblColNameValue);

					int numberOfDeletions = positions.size();
					changeNoRows(strTableName, "-", numberOfDeletions);
					if (e.isEmpty()) {
						File f = new File(currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");
						f.delete();
						changeNoPages(strTableName, "-", PAGES.get(i).intValue());
					} else {
						serializingAnObject(e,
								currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");
					}
					for (int k : positions) {
						updateTheIndexFromDelete(strTableName, e.getPageName(), indexedColumns, htblColNameValue, k);
					}
				}
			}
		} else {

			File folder = new File(currentDir + "\\data\\" + strTableName);
			File[] listOfFiles = folder.listFiles();
			Bitmap b = null;
			ArrayList<ArrayList<String>> multiVal = new ArrayList<>();
			for (int h = 0; h < indexedCol.size(); h++) {
				for (int i = 0; i < listOfFiles.length; i++) {
					if (listOfFiles[i].getName().contains(indexedCol.get(h))) {

						b = (Bitmap) deserializingAnObject(listOfFiles[i].getPath());
						if (b.bitmapContainsKey(htblColNameValue.get(indexedCol.get(h)))) {
							Vector<Hashtable<Object, ArrayList<Triple>>> allElements = b.getAllBitmaps();
							for (int j = 0; j < allElements.size(); j++) {
								if (allElements.get(j).keys().nextElement().toString()
										.equals(htblColNameValue.get(indexedCol.get(h)))) {
									multiVal.add(decomporesssIndexedCol(listOfFiles[i].getPath(),
											allElements.get(j).keys().nextElement().toString(), j));
									break;
								}
							}

							break;
						}

					}
				}

			}
			System.out.println("here");
			for (int ba = 0; ba < multiVal.size(); ba++) {
				System.out.println(multiVal.get(ba));

			}
			ArrayList<String> allTogether = AND(multiVal);
			// System.out.println("hahahahhahahhaha");
			// System.out.println(allTogether.toString());
			ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
			for (int i = 0; i < PAGES.size(); i++) {

				if (allTogether.get(PAGES.get(i).intValue()).contains("1")) {
					Page e = (Page) deserializingAnObject(
							currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
					if (e != null) {
						ArrayList<Integer> positions = e.removeRecord(value);
						ArrayList<String> indexedColumns = getAllIndex(strTableName, htblColNameValue);

						int numberOfDeletions = positions.size();
						changeNoRows(strTableName, "-", numberOfDeletions);
						if (e.isEmpty()) {
							File f = new File(currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");
							f.delete();
							changeNoPages(strTableName, "-", PAGES.get(i).intValue());
						} else {
							serializingAnObject(e,
									currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");
						}
						for (int k : positions) {
							updateTheIndexFromDelete(strTableName, e.getPageName(), indexedColumns, htblColNameValue,
									k);
						}
					}

				}
			}

		}
	}
	////////////////////////////////////////////////////////////// BITMAP INDEX
	////////////////////////////////////////////////////////////// ////////////////////////////////////////////////////////////

	private ArrayList<Object> getDistinctValues(String tableName, String colName) {

		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + tableName + "\\PAGES.ser");
		ArrayList<Object> distinctValues = new ArrayList<>();

		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + tableName + "\\Page" + PAGES.get(i) + ".ser");
			Vector<Hashtable<String, Object>> storage = e.getValues();
			for (int j = 0; j < storage.size(); j++) {
				Hashtable<String, Object> record = storage.get(j);
				Object o = record.get(colName);

				if (!distinctValues.contains(o))
					distinctValues.add(o);
			}
		}

		return distinctValues;
	}

	/*
	 *  
	 * 
	 * 
	 * */

	public int getNumberOfCreatedBitmaps(String strTableName, String strColName) {

		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		int num = 0;
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().charAt(0) == 'B') {
				num = (int) (Math.max(num, Integer
						.parseInt(listOfFiles[i].getName().substring(6, listOfFiles[i].getName().length() - 4))));

			}
		}
		return num;

	}

	public void createBitmapIndex(String strTableName, String strColName) throws DBAppException {
		File ff = new File(currentDir + "\\data\\" + strTableName + "\\metadata.csv");

		try {

			BufferedReader br = new BufferedReader(new FileReader(ff));
			String theData = "";
			String line;
			while ((line = br.readLine()) != null) {
				theData += line + "\n";

			}
			br.close();
			PrintWriter pw = new PrintWriter(ff.getPath());
			BufferedReader brr = new BufferedReader(new StringReader(theData));
			while ((line = brr.readLine()) != null) {
				String[] linee = line.split(",");
				if (linee[1].equals(strColName)) {
					linee[4] = "TRUE";
				}
				String koko = "";
				for (int i = 0; i < linee.length; i++) {
					koko += linee[i] + ",";
				}
				pw.println(koko);
			}
			pw.flush();
			pw.close();

		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e2) {
			e2.printStackTrace();
		}

		ArrayList<Object> distinct = getDistinctValues(strTableName, strColName);

		if (distinct.get(0) instanceof Integer) {
			ArrayList<Integer> in = new ArrayList<>();
			for (int i = 0; i < distinct.size(); i++) {
				in.add((Integer) distinct.get(i));
			}
			Collections.sort(in);
			for (int i = 0; i < in.size(); i++) {
				distinct.set(i, in.get(i));
			}
		} else if (distinct.get(0) instanceof Double) {
			ArrayList<Double> in = new ArrayList<>();
			for (int i = 0; i < distinct.size(); i++) {
				in.add((Double) distinct.get(i));
			}
			Collections.sort(in);
			for (int i = 0; i < in.size(); i++) {
				distinct.set(i, in.get(i));
			}
		} else if (distinct.get(0) instanceof String) {
			ArrayList<String> in = new ArrayList<>();
			for (int i = 0; i < distinct.size(); i++) {
				in.add((String) distinct.get(i));
			}
			Collections.sort(in);
			for (int i = 0; i < in.size(); i++) {
				distinct.set(i, in.get(i));
			}
		}

		String[] bitmaps = new String[distinct.size()];
		Arrays.fill(bitmaps, "");
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\Page" + PAGES.get(i) + ".ser");
			Vector<Hashtable<String, Object>> storage = e.getValues();
			for (int f = 0; f < distinct.size(); f++) {
				bitmaps[f] = bitmaps[f] + e.getPageName() + ",";
			}
			for (int j = 0; j < storage.size(); j++) {
				Hashtable<String, Object> record = storage.get(j);
				Object o = record.get(strColName);
				int index = distinct.indexOf(o);

				for (int k = 0; k < distinct.size(); k++) {
					if (k == index)
						bitmaps[k] = bitmaps[k] + "1";
					else
						bitmaps[k] = bitmaps[k] + "0";

				}

			}
			for (int f = 0; f < distinct.size(); f++) {
				bitmaps[f] = bitmaps[f] + ",";
			}
		}

		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(currentDir + "\\config\\config.properties"));
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		String value = properties.getProperty("maxNumberRows");
		int max = Integer.parseInt(value);
		for (int j = 1; j <= (int) Math.ceil(1.0 * bitmaps.length / max); j++) {

			Bitmap newBitmap = new Bitmap(strTableName, j, this, strColName);
			int i = 0;
			for (; i < (int) Math.min(max, bitmaps.length); i++) {

				ArrayList<Triple> bitmapOfValue = compressRLE(bitmaps[i]);
				Hashtable<Object, ArrayList<Triple>> bitmap = new Hashtable<>();
				// System.out.println(distinct.get(i));
				bitmap.put(distinct.get(i), bitmapOfValue);

				newBitmap.addElement(bitmap);
			}
			serializingAnObject(newBitmap,
					currentDir + "\\data\\" + strTableName + "\\" + newBitmap.getBitmapName() + ".ser");
		}

	}

	public ArrayList<Triple> compressRLE(String zeroAndOne) {

		String compressed = "";
		StringTokenizer st = new StringTokenizer(zeroAndOne, ",");
		ArrayList<Triple> compressForm = new ArrayList<>();
		while (st.hasMoreTokens()) {
			String pageName = st.nextToken();
			if (!st.hasMoreTokens())
				break;
			String simplifiedForm = st.nextToken();
			char currentChar = simplifiedForm.charAt(0);
			int occurances = 1;
			for (int i = 1; i <= simplifiedForm.length() - 1; i++) {

				if (currentChar != simplifiedForm.charAt(i)) {
					Triple p = new Triple(pageName, occurances, Integer.parseInt(currentChar + ""));
					compressForm.add(p);
					occurances = 1;
					currentChar = simplifiedForm.charAt(i);

				} else {
					occurances++;
				}
			}
			Triple p = new Triple(pageName, occurances, Integer.parseInt(currentChar + ""));
			compressForm.add(p);

		}

		return compressForm;
	}

	public String decompressRLE(Triple p) {
		String decompressedForm = "";
		int numberOfVal = p.getNumberOfValue();
		for (int i = 0; i < numberOfVal; i++) {
			decompressedForm += p.getValue();
		}

		return decompressedForm;
	}

	public Bitmap whichBitmap(String strTableName, Object newBitmapVal, String Column) {
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		Bitmap b = null;
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().contains(Column)) {
				b = (Bitmap) deserializingAnObject(listOfFiles[i].getPath());
				Object lastVal = b.getLastElementKey();
				if (lastVal.toString().compareTo(newBitmapVal.toString()) > 0) {
					return b;
				}

			}
		}

		return b;
	}

	public Bitmap lastBitmap(String strTableName, String strColName) {
		File file = new File(currentDir + "\\data\\" + strTableName);
		String[] directories = file.list(new FilenameFilter() {
			@Override
			public boolean accept(File current, String name) {
				if (name.contains(strColName)) {
					return true;
				}
				return false;
			}
		});
		return (Bitmap) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\" + directories[directories.length - 1]);
	}

	public Hashtable<Object, ArrayList<Triple>> BitmappingValue(String strTableName, Object newVal, String strColName) {
		String bitmap = "";
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\Page" + PAGES.get(i) + ".ser");
			Vector<Hashtable<String, Object>> storage = e.getValues();
			bitmap += e.getPageName() + ",";

			for (int j = 0; j < storage.size(); j++) {
				Hashtable<String, Object> record = storage.get(j);
				Object o = record.get(strColName);

				if (newVal.toString().equals(o.toString())) {
					bitmap = bitmap + "1";
				} else
					bitmap = bitmap + "0";

			}
			bitmap += ",";

		}

		ArrayList<Triple> bitmapOfValue = compressRLE(bitmap);
		Hashtable<Object, ArrayList<Triple>> Bitmap = new Hashtable<>();
		Bitmap.put(newVal, bitmapOfValue);
		return Bitmap;

	}

	private Bitmap createBitmapFile(String strTableName, String strColumnName) {
		File file = new File(currentDir + "\\data\\" + strTableName);
		String[] directories = file.list(new FilenameFilter() {
			@Override
			public boolean accept(File current, String name) {
				if (name.charAt(0) == 'B') {
					return true;
				}
				return false;
			}
		});
		int lastVal = Integer.parseInt(
				directories[directories.length - 1].substring(6, directories[directories.length - 1].length() - 4)) + 1;
		return new Bitmap(strTableName, lastVal, this, strColumnName);

	}

	/*
	 * 
	 * 
	 * please check this method !!
	 * 
	 * 
	 */
	// public void updateBitmapObject(String strTableName, Object val, String
	// Column) {
	// File folder = new File(currentDir + "\\data\\" + strTableName);
	// File[] listOfFiles = folder.listFiles();
	// Bitmap b = null;
	// for (int i = 0; i < listOfFiles.length; i++) {
	// if (listOfFiles[i].getName().contains(Column)) {
	// b = (Bitmap) deserializingAnObject(listOfFiles[i].getPath());
	// if (b.removeRecord(val)) {
	// break;
	// }
	//
	// }
	// }
	//
	// }
	public boolean deleteBitmapObject(String strTableName, Object val, String Column) {
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		Bitmap b = null;
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().contains(Column)) {
				b = (Bitmap) deserializingAnObject(listOfFiles[i].getPath());
				if (b.removeRecord(val)) {
					serializingAnObject(b, listOfFiles[i].getPath());
					return true;
				}

			}
		}
		return false;
	}

	public void insertNewBitmapObject(String strTableName, Object newBitmapVal, String strColName) {

		Object value = newBitmapVal;
		Hashtable<Object, ArrayList<Triple>> theBitmappedNewValue = BitmappingValue(strTableName, newBitmapVal,
				strColName);
		Hashtable<Object, ArrayList<Triple>> lastBitmapVal = null;

		Bitmap lastBitmap = lastBitmap(strTableName, strColName);
		while (true) {
			Bitmap toAddIn = whichBitmap(strTableName, value, strColName);

			if (toAddIn.isFull() && toAddIn.getBitmapName().equals(lastBitmap.getBitmapName())) {

				Bitmap b = createBitmapFile(strTableName, strColName);
				if (lastBitmap.getLastElementKey().toString().compareTo(value.toString()) < 0) {

					b.addElement(theBitmappedNewValue);

				} else {
					lastBitmapVal = lastBitmap.removeLastElement();
					lastBitmap.addElement(theBitmappedNewValue);
					b.addElement(lastBitmapVal);
				}
				serializingAnObject(lastBitmap,
						currentDir + "\\data\\" + strTableName + "\\" + lastBitmap.getBitmapName() + ".ser");
				serializingAnObject(b, currentDir + "\\data\\" + strTableName + "\\" + b.getBitmapName() + ".ser");

				break;
			} else if (toAddIn.isFull()) {

				lastBitmapVal = toAddIn.removeLastElement();
				toAddIn.addElement(theBitmappedNewValue);
				serializingAnObject(toAddIn,
						currentDir + "\\data\\" + strTableName + "\\" + toAddIn.getBitmapName() + ".ser");
				theBitmappedNewValue = lastBitmapVal;
				value = theBitmappedNewValue.keys().nextElement();
			} else {
				toAddIn.addElement(theBitmappedNewValue);
				serializingAnObject(toAddIn,
						currentDir + "\\data\\" + strTableName + "\\" + toAddIn.getBitmapName() + ".ser");
				break;
			}

		}

	}

	public void deleteValueBitmapIndex(String strTableName, String pageName, String Column, Object val, int position) {
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().contains(Column)) {
				Bitmap b = (Bitmap) deserializingAnObject(
						currentDir + "\\data\\" + strTableName + "\\" + listOfFiles[i].getName());
				Vector<Hashtable<Object, ArrayList<Triple>>> allElements = b.getAllBitmaps();
				for (int j = 0; j < allElements.size(); j++) {
					Hashtable<Object, ArrayList<Triple>> record = allElements.get(i);
					Object key = record.keys().nextElement();
					ArrayList<Triple> arr = record.get(key);

					for (int k = 0; k < arr.size(); k++) {
						int cumulative = 0;
						if (arr.get(k).getPageName().equals(pageName)) {
							if (position > cumulative) {
								cumulative += arr.get(k).getNumberOfValue();
							} else {
								String decompressed = decompressRLE(arr.get(k));
								arr.remove(k);
								int index = position - cumulative;
								String modified = "";
								if (val.toString().equals(key.toString())) {
									modified = decompressed.substring(0, index) + decompressed.substring(index + 1);

								} else {
									modified = decompressed.substring(0, index) + decompressed.substring(index + 1);

								}
								ArrayList<Triple> compressed = compressRLE(modified);

								arr.addAll(k, compressed);
							}
						}
					}

				}
				b.setAllBitmaps(allElements);
				serializingAnObject(b, currentDir + "\\data\\" + strTableName + "\\" + b.getBitmapName() + ".ser");
			}
		}

	}

	public void insertValueBitmapIndex(String strTableName, String pageName, String Column, Object val, int position) {
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().contains(Column)) {
				Bitmap b = (Bitmap) deserializingAnObject(
						currentDir + "\\data\\" + strTableName + "\\" + listOfFiles[i].getName());
				Vector<Hashtable<Object, ArrayList<Triple>>> allElements = b.getAllBitmaps();
				for (int j = 0; j < allElements.size(); j++) {
					Hashtable<Object, ArrayList<Triple>> record = allElements.get(i);
					Object key = record.keys().nextElement();
					ArrayList<Triple> arr = record.get(key);

					for (int k = 0; k < arr.size(); k++) {
						int cumulative = 0;
						if (arr.get(k).getPageName().equals(pageName)) {
							if (position > cumulative) {
								cumulative += arr.get(k).getNumberOfValue();
							} else {
								String decompressed = decompressRLE(arr.get(k));
								arr.remove(k);
								int index = position - cumulative;
								String modified = "";
								if (val.toString().equals(key.toString())) {
									modified = decompressed.substring(0, index) + "1" + decompressed.substring(index);

								} else {
									modified = decompressed.substring(0, index) + "0" + decompressed.substring(index);

								}
								ArrayList<Triple> compressed = compressRLE(modified);

								arr.addAll(k, compressed);
							}
						}
					}

				}
				b.setAllBitmaps(allElements);
				serializingAnObject(b, currentDir + "\\data\\" + strTableName + "\\" + b.getBitmapName() + ".ser");

			}
		}

	}

	static class Triple implements Serializable {
		private String pageName;
		private int numberOfValue;
		private int value;

		public Triple(String pageName, int numberValues, int val) {
			this.pageName = pageName;
			this.numberOfValue = numberValues;
			this.value = val;
		}

		public int getPageNumber() {
			return Integer.parseInt(pageName.substring(4));
		}

		public String getPageName() {
			return pageName;
		}

		public void setPageName(String pageName) {
			this.pageName = pageName;
		}

		public int getNumberOfValue() {
			return numberOfValue;
		}

		public void setNumberOfValue(int numberOfValue) {
			this.numberOfValue = numberOfValue;
		}

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}
		// public String toString () {
		// return "{ "+this.pageName + ", " + this.numberOfValue + " ," +
		// this.value + " } " ;
		// }

	}

	/////////////////////////////////////////////////////////////////////////////////// SELECTION
	//////////////////////////////////////////////////////////////////////////////////
	private ArrayList<String> decomporesssIndexedCol(String Path, Object key, int index) {
		ArrayList<String> decompressedValues = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			decompressedValues.add("");
		}
		Bitmap b = (Bitmap) deserializingAnObject(Path);

		ArrayList<Triple> comval = b.getElementTriple(key, index);

		for (int i = 0; i < comval.size(); i++) {
			if (i == 0) {
				decompressedValues.set(comval.get(i).getPageNumber(), decompressRLE(comval.get(i)));
			} else {
				String already = decompressedValues.get(comval.get(i).getPageNumber()) + decompressRLE(comval.get(i));
				decompressedValues.set(comval.get(i).getPageNumber(), already);

			}
		}
		System.out.println(decompressedValues.toString());
		return decompressedValues;
	}

	private ArrayList<String> fillWithZeros(String strTableName) {
		ArrayList<String> res = new ArrayList<>();
		for (int j = 0; j < 10000; j++) {
			res.add("");
		}
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			char [] zeros = new char [e.getNumberOfRows()] ; 
			Arrays.fill(zeros, '0');
			String zer = new String (zeros ) ; 
			res.set(PAGES.get(i).intValue(),zer) ; 
			
		}
		return res;
	}

	public String padding(String result, int pad) {

		while (result.length() < pad) {
			result = "0" + result;
		}
		return result;
	}

	private ArrayList<String> AND(ArrayList<ArrayList<String>> decompressedVal) {

		ArrayList<String> andResult = null;
		for (int i = 0; i < decompressedVal.size(); i++) {
			if (decompressedVal.get(i) != null && andResult == null) {
				andResult = decompressedVal.get(i);
			} else if (decompressedVal.get(i) != null)
				for (int j = 0; j < decompressedVal.get(i).size(); j++) {
					if (decompressedVal.get(i).get(j) != null && decompressedVal.get(i).get(j).length() != 0) {
						BigInteger a = new BigInteger(decompressedVal.get(i).get(j), 2);
						String result = a.and(new BigInteger(andResult.get(j), 2)).toString(2);
						result = padding(result, andResult.get(j).length());
						andResult.set(j, result);
					}
				}
		}
		return andResult;
	}

	private ArrayList<String> OR(ArrayList<ArrayList<String>> decompressedVal) {
		ArrayList<String> orResult = null;
		for (int i = 0; i < decompressedVal.size(); i++) {
			if (decompressedVal.get(i) != null && orResult == null) {
				orResult = decompressedVal.get(i);
			} else if (decompressedVal.get(i) != null)
				for (int j = 0; j < decompressedVal.get(i).size(); j++) {
					if (decompressedVal.get(i).get(j) != null && decompressedVal.get(i).get(j).length() != 0) {

						BigInteger a = new BigInteger(decompressedVal.get(i).get(j), 2);
						String result = a.or(new BigInteger(orResult.get(j), 2)).toString(2);
						result = padding(result, orResult.get(j).length());
						orResult.set(j, result);
					}
				}
		}
	
		return orResult;
	}

	private ArrayList<String> XOR(ArrayList<ArrayList<String>> decompressedVal) {
		ArrayList<String> xorResult = null;
		for (int i = 0; i < decompressedVal.size(); i++) {
			if (decompressedVal.get(i) != null && xorResult == null) {
				xorResult = decompressedVal.get(i);
			} else if (decompressedVal.get(i) != null)
				for (int j = 0; j < decompressedVal.get(i).size(); j++) {
					if (decompressedVal.get(i).get(j) != null && decompressedVal.get(i).get(j).length() != 0) {
						BigInteger a = new BigInteger(decompressedVal.get(i).get(j), 2);
						String result = a.xor(new BigInteger(xorResult.get(j), 2)).toString(2);
						result = padding(result, xorResult.get(j).length());
						xorResult.set(j, result);
					}
				}
		}
		return xorResult;
	}

	private ArrayList<String> lessThan(String strTableName, String strColName, Object GOAL) {

		ArrayList<ArrayList<String>> ALL = new ArrayList<>();
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().contains(strColName)) {
				String Path = currentDir + "\\data\\" + strTableName + "\\" + listOfFiles[i].getName();
				Bitmap b = (Bitmap) deserializingAnObject(Path);
				Vector<Hashtable<Object, ArrayList<Triple>>> sto = b.getAllBitmaps();
				for (int j = 0; j < sto.size(); j++) {
					Object insta = sto.get(j).keys().nextElement();
				
					if (insta.toString().compareTo(GOAL.toString()) < 0) {
						ALL.add(decomporesssIndexedCol(Path, insta, j));
					}
				}

			}
		}
		return ALL.size()!=0 ? OR(ALL) : fillWithZeros(strTableName) ;
	}

	private ArrayList<String> lessThanOrEqual(String strTableName, String strColName, Object GOAL) {

		ArrayList<ArrayList<String>> ALL = new ArrayList<>();
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().contains(strColName)) {
				String Path = currentDir + "\\data\\" + strTableName + "\\" + listOfFiles[i].getName();
				Bitmap b = (Bitmap) deserializingAnObject(Path);
				Vector<Hashtable<Object, ArrayList<Triple>>> sto = b.getAllBitmaps();
				for (int j = 0; j < sto.size(); j++) {
					Object insta = sto.get(j).keys().nextElement();
					if (insta.toString().compareTo(GOAL.toString()) <= 0) {
						ALL.add(decomporesssIndexedCol(Path, insta, j));

					}
				}

			}
		}
		return ALL.size()!=0 ? OR(ALL) : fillWithZeros(strTableName) ;
	}

	private ArrayList<String> Equal(String strTableName, String strColName, Object GOAL) {
		// TODO
		ArrayList<String> result = new ArrayList<>();
		// ArrayList<ArrayList<String>> ALL = new ArrayList<>();
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().contains(strColName)) {
				String Path = currentDir + "\\data\\" + strTableName + "\\" + listOfFiles[i].getName();
				Bitmap b = (Bitmap) deserializingAnObject(Path);
				Vector<Hashtable<Object, ArrayList<Triple>>> sto = b.getAllBitmaps();
				Object[] dis = new Object[sto.size()];

				for (int j = 0; j < sto.size(); j++) {
					// System.out.println(sto.get(j).keys().nextElement().toString()
					// + " ya lllaaaayeeettttttt");
					dis[j] = sto.get(j).keys().nextElement().toString();
				}
				int index = Arrays.binarySearch(dis, GOAL);
				// System.out.println(GOAL);
				// System.out.println(Arrays.toString(dis));
				// System.out.println(index +
				// "INNNNNNNNNNNNNNNNNNNNDDDDDDDDDDDDEEEEEEEEEEEEXXXXXXXXXXXXXX");
				if (index >= 0) {
					result = decomporesssIndexedCol(Path, GOAL, index);
					// System.out.println(result.toString());
				}
				else {
					result = fillWithZeros(strTableName) ; 
				}

			}
		}
		return result;
	}

	private ArrayList<String> GreaterOrEqual(String strTableName, String strColName, Object GOAL) {

		ArrayList<ArrayList<String>> ALL = new ArrayList<>();
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().contains(strColName)) {
				String Path = currentDir + "\\data\\" + strTableName + "\\" + listOfFiles[i].getName();
				Bitmap b = (Bitmap) deserializingAnObject(Path);
				Vector<Hashtable<Object, ArrayList<Triple>>> sto = b.getAllBitmaps();
				for (int j = 0; j < sto.size(); j++) {
					Object insta = sto.get(j).keys().nextElement();
					if (insta.toString().compareTo(GOAL.toString()) > 0) {
						ALL.add(decomporesssIndexedCol(Path, insta, j));

					}
				}

			}
		}
		return ALL.size()!=0 ? OR(ALL) : fillWithZeros(strTableName) ;
	}

	private ArrayList<String> GreaterThan(String strTableName, String strColName, Object GOAL) {

		ArrayList<ArrayList<String>> ALL = new ArrayList<>();
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().contains(strColName)) {
				String Path = currentDir + "\\data\\" + strTableName + "\\" + listOfFiles[i].getName();
				Bitmap b = (Bitmap) deserializingAnObject(Path);
				Vector<Hashtable<Object, ArrayList<Triple>>> sto = b.getAllBitmaps();
				// System.out.println(sto.size());
				// Set keySet = sto.get(1).keySet() ;
				// Iterator it = keySet.iterator() ;
				// while (it.hasNext())
				// System.out.println(it.next().toString());
				// System.out.println(sto.get(2).keys().nextElement().toString()
				// + " KEEEEEEEEEEEEEEEEEEEYSSS");
				for (int j = 0; j < sto.size(); j++) {
					Object insta = sto.get(j).keys().nextElement();

					if (insta.toString().compareTo(GOAL.toString()) > 0) {
						ALL.add(decomporesssIndexedCol(Path, insta, j));

					}
				}

			}
		}
		// System.out.println(ALL.get(3).toString());
		// System.out.println(ALL.size() + " aaaaaaaaaaaaaaasdasdasdasdasdas");
		return ALL.size()!=0 ? OR(ALL) : fillWithZeros(strTableName) ;
	}

	private ArrayList<String> NotEqual(String strTableName, String strColName, Object GOAL) {

		ArrayList<String> result = Equal(strTableName, strColName, GOAL);
		String xr = "";
		for (int i = 0; i < result.size(); i++) {
			if (result.get(i) != null && result.get(i).length() != 0) {
				if (xr.length() == 0) {
					char[] n = new char[result.get(i).length()];
					Arrays.fill(n, '1');
					xr = new String(n);
				}
				BigInteger a = new BigInteger(result.get(i), 2);
				String notRes = a.xor(new BigInteger(xr, 2)).toString(2);
				int max = result.get(i).length();
				result.set(i, padding(notRes, max));
			}
		}
		return result;

	}

	//////////////////////////////// NOT INDEXED SUB METHOD
	//////////////////////////////// ////////////////////////////////////////////////////////////////

	private ArrayList<String> lessThanNotIndexed(String strTableName, String strColName, Object GOAL) {

		ArrayList<String> result = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			result.add("");
		}
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			Vector<Hashtable<String, Object>> theElements = e.getValues();
			String value = "";
			for (int j = 0; j < theElements.size(); j++) {
				if (theElements.get(j).get(strColName).toString().compareTo(GOAL.toString()) < 0) {
					value += 1;
				} else {
					value += 0;
				}
			}
			result.set(PAGES.get(i).intValue(), value);
		}
		return result;

	}

	private ArrayList<String> lessThanOrEqualNotIndexed(String strTableName, String strColName, Object GOAL) {

		ArrayList<String> result = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			result.add("");
		}
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			Vector<Hashtable<String, Object>> theElements = e.getValues();
			String value = "";
			for (int j = 0; j < theElements.size(); j++) {
				if (theElements.get(j).get(strColName).toString().compareTo(GOAL.toString()) <= 0) {
					value += 1;
				} else {
					value += 0;
				}
			}
			result.set(PAGES.get(i).intValue(), value);
		}
		return result;

	}

	private ArrayList<String> EqualNotIndexed(String strTableName, String strColName, Object GOAL) {

		ArrayList<String> result = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			result.add("");
		}
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			Vector<Hashtable<String, Object>> theElements = e.getValues();
			String value = "";
			for (int j = 0; j < theElements.size(); j++) {
				if (theElements.get(j).get(strColName).toString().compareTo(GOAL.toString()) == 0) {
					value += 1;
				} else {
					value += 0;
				}
			}
			// System.out.println(PAGES.get(i).intValue());
			result.set(PAGES.get(i).intValue(), value);
		}
		return result;

	}

	private ArrayList<String> GreaterOrEqualNotIndexed(String strTableName, String strColName, Object GOAL) {

		ArrayList<String> result = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			result.add("");
		}
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			Vector<Hashtable<String, Object>> theElements = e.getValues();
			String value = "";
			for (int j = 0; j < theElements.size(); j++) {
				if (theElements.get(j).get(strColName).toString().compareTo(GOAL.toString()) >= 0) {
					value += 1;
				} else {
					value += 0;
				}
			}
			result.set(PAGES.get(i).intValue(), value);
		}
		return result;

	}

	private ArrayList<String> GreaterThanNotIndexed(String strTableName, String strColName, Object GOAL) {

		ArrayList<String> result = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			result.add("");
		}
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			Vector<Hashtable<String, Object>> theElements = e.getValues();
			String value = "";
			for (int j = 0; j < theElements.size(); j++) {
				if (theElements.get(j).get(strColName).toString().compareTo(GOAL.toString()) > 0) {
					value += 1;
				} else {
					value += 0;
				}
			}
			result.set(PAGES.get(i).intValue(), value);
		}
		return result;

	}

	private ArrayList<String> NotEqualNotIndexed(String strTableName, String strColName, Object GOAL) {

		ArrayList<String> result = EqualNotIndexed(strTableName, strColName, GOAL);
		String xr = "";
		for (int i = 0; i < result.size(); i++) {
			if (result.get(i) != null && result.get(i).length() != 0) {
				if (xr.length() == 0) {
					char[] n = new char[result.get(i).length()];
					Arrays.fill(n, '1');
					xr = new String(n);
				}
				BigInteger a = new BigInteger(result.get(i), 2);
				String notRes = a.xor(new BigInteger(xr, 2)).toString(2);
				int max = result.get(i).length();

				result.set(i, padding(notRes, max));
			}
		}
		return result;

	}

	/////////////////////////// NOT INDEXED SUB METHOD
	/////////////////////////// ////////////////////////////////////////////////////////////////

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		if (arrSQLTerms.length - 1 != strarrOperators.length) {
			throw new DBAppException("BAD SYNTAX");
		}
		String tableName = arrSQLTerms[0]._strTableName;
		// System.out.println(tableName);
		Hashtable<String, Object> input = new Hashtable<>();

		for (SQLTerm term : arrSQLTerms) {
			input.put(term._strColumnName, term._objValue);
			term.isValidOperator();
			if (!tableName.equals(term._strTableName)) {
				throw new DBAppException("OUR DATABASE DOES NOT SUPPORT JOIN OPERATIONS ");
			}
		}
		validateTypesOfValues(input, tableName);
		ArrayList<String> operators = new ArrayList<>();
		for (String op : strarrOperators) {
			operators.add(op);
		}
		ArrayList<String> indexedCol = getAllIndex(tableName, input);
		// System.out.println(indexedCol.toString());
		// System.out.println(input);
		ArrayList<ArrayList<String>> theSqlTerms = new ArrayList<>();
		for (int i = 0; i < arrSQLTerms.length; i++) {
			SQLTerm term = arrSQLTerms[i];
			if (indexedCol.contains(term._strColumnName)) {
				switch (term._strOperator) {
				case ">":
					// System.out.println("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk");
					theSqlTerms.add(GreaterThan(term._strTableName, term._strColumnName, term._objValue));
					break;
				case "<":
					theSqlTerms.add(lessThan(term._strTableName, term._strColumnName, term._objValue));

					break;
				case ">=":
					// System.out.println("Hallloo4");
					theSqlTerms.add(GreaterOrEqual(term._strTableName, term._strColumnName, term._objValue));

					break;
				case "<=":
					theSqlTerms.add(lessThanOrEqual(term._strTableName, term._strColumnName, term._objValue));

					break;
				case "=":
					// System.out.println(Equal(term._strTableName,
					// term._strColumnName, term._objValue).toString()
					// + "aaaaaaaaaadasdasdaaaaaaaaaaaaaaa");
					theSqlTerms.add(Equal(term._strTableName, term._strColumnName, term._objValue));

					break;
				case "!=":
					theSqlTerms.add(NotEqual(term._strTableName, term._strColumnName, term._objValue));

					break;

				default:
					break;
				}
			} else {

				switch (term._strOperator) {
				case ">":
					theSqlTerms.add(GreaterThanNotIndexed(term._strTableName, term._strColumnName, term._objValue));
					break;
				case "<":
					theSqlTerms.add(lessThanNotIndexed(term._strTableName, term._strColumnName, term._objValue));

					break;
				case ">=":
					theSqlTerms.add(GreaterOrEqualNotIndexed(term._strTableName, term._strColumnName, term._objValue));

					break;
				case "<=":
					theSqlTerms.add(lessThanOrEqualNotIndexed(term._strTableName, term._strColumnName, term._objValue));

					break;
				case "=":
					theSqlTerms.add(EqualNotIndexed(term._strTableName, term._strColumnName, term._objValue));

					break;
				case "!=":
					theSqlTerms.add(NotEqualNotIndexed(term._strTableName, term._strColumnName, term._objValue));

					break;

				default:
					break;
				}

			}
		}
		while (operators.contains("AND")) {
			int index = operators.indexOf("AND");
			ArrayList<ArrayList<String>> twoTerms = new ArrayList<>();

			twoTerms.add(theSqlTerms.get(index));
			twoTerms.add(theSqlTerms.get(index + 1));

			ArrayList<String> andedTerms = AND(twoTerms);
			operators.remove(index);
			theSqlTerms.remove(index);
			theSqlTerms.remove(index );
			theSqlTerms.add(index, andedTerms);

		}
		while (operators.contains("XOR")) {
			int index = operators.indexOf("XOR");
			ArrayList<ArrayList<String>> twoTerms = new ArrayList<>();
			twoTerms.add(theSqlTerms.get(index));
			twoTerms.add(theSqlTerms.get(index + 1));

			ArrayList<String> xoredTerms = XOR(twoTerms);
			operators.remove(index);
			theSqlTerms.remove(index);
			theSqlTerms.remove(index );
			theSqlTerms.add(index, xoredTerms);

		}
		while (operators.contains("OR")) {
			int index = operators.indexOf("OR");
			ArrayList<ArrayList<String>> twoTerms = new ArrayList<>();
			twoTerms.add(theSqlTerms.get(index));
			twoTerms.add(theSqlTerms.get(index + 1));

			ArrayList<String> oredTerms = OR(twoTerms);
			operators.remove(index);
			theSqlTerms.remove(index);
			theSqlTerms.remove(index );
			theSqlTerms.add(index, oredTerms);

		}
		ArrayList<Hashtable<String, Object>> result = new ArrayList<>();
		ArrayList<String> theFinalSQL = theSqlTerms.get(0);
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + tableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + tableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			Vector<Hashtable<String, Object>> theElements = e.getValues();
			String pageVal = theFinalSQL.get(PAGES.get(i).intValue());
			for (int j = 0; j < pageVal.length(); j++) {
				if (pageVal.charAt(j) == '1') {
					result.add(theElements.get(j));
				}
			}
		}

		return result.iterator();
	}
}
