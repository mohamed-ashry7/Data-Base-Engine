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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Pattern;

public class DBApp implements Serializable {
	String currentDir = System.getProperty("user.dir");

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

		checkIfExists(strTableName);

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
				currentDir + "\\data\\" + strTableName + "\\DATA.ser");
		String PAGES = (String) DATA.get(0);
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

	private Page whichBlock(Hashtable<String, Object> record, String clustered, String Path) {
		Page lastPage = null;
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(Path + "\\PAGES.ser");
		Object value = record.get(clustered);

		for (int i = 0; i < PAGES.size(); i++) {

			String path = Path + "\\" + "Page" + PAGES.get(i).intValue() + ".ser";
			Page deserializedPage = (Page) deserializingAnObject(path);
			if (deserializedPage != null) {
				if (deserializedPage.isEmpty()) {
					System.out.println(3);

					return deserializedPage;
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
		Page e = new Page(strTableName, numberOfCreatedPages, this);
		e.setClustering(clusteringColumn, clusteringColumnType);
		changeNoPages(strTableName, "+", numberOfCreatedPages);
		System.out.println(currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");
		serializingAnObject(e, currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");
		return e;
	}

	///////////////////////////////// INSERT
	/////////////////////////////////////////////////////////////////////////////////
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		/*
		 * you must check if the column indexed and if it is you must check the
		 * new values if it is unique or no
		 */
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
		Page lastPage = lastPage(currentDir + "\\data\\" + strTableName);
		while (true) {
			Page toAddIn = whichBlock(value, clusteringColumn, currentDir + "\\data\\" + strTableName);

			if (toAddIn.isFull() && toAddIn.getPageName().equals(lastPage.getPageName())) {
				System.out.println(11);

				Page e = createPage(strTableName, htblColNameValue);
				if (lastPage.getLastElement().get(clusteringColumn).toString()
						.compareTo(value.get(clusteringColumn).toString()) < 0) {

					e.addElement(value);

				} else {
					lastValue = lastPage.removeLastElement();
					lastPage.addElement(value);
					e.addElement(lastValue);
				}
				serializingAnObject(lastPage,
						currentDir + "\\data\\" + strTableName + "\\" + lastPage.getPageName() + ".ser");
				serializingAnObject(e, currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");

				break;
			} else if (toAddIn.isFull()) {
				System.out.println(22);

				lastValue = toAddIn.removeLastElement();
				toAddIn.addElement(value);
				serializingAnObject(toAddIn,
						currentDir + "\\data\\" + strTableName + "\\" + toAddIn.getPageName() + ".ser");
				value = lastValue;
			} else {
				System.out.println(33);
				toAddIn.addElement(value);
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

		String clusteringValue = strKey;
		Hashtable<String, Object> value = mapHash(htblColNameValue);
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			if (e != null) {
				e.updateRecord(clusteringValue, htblColNameValue);
				serializingAnObject(e, currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");

			}
		}

	}

	////////////////////////////////////// DELETE//////////////////////////////////////////////////////////////////
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		validateTypesOfValues(htblColNameValue, strTableName);

		String clusteringColumn = clusteringColumn(strTableName);
		Hashtable<String, Object> value = mapHash(htblColNameValue);
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\" + "Page" + PAGES.get(i).intValue() + ".ser");
			if (e != null) {
				int numberOfDeletions = e.removeRecord(value);
				changeNoRows(strTableName, "-", numberOfDeletions);
				if (e.isEmpty()) {
					File f = new File(currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");
					f.delete();
					changeNoPages(strTableName, "-", PAGES.get(i).intValue());
				} else {
					serializingAnObject(e, currentDir + "\\data\\" + strTableName + "\\" + e.getPageName() + ".ser");
				}
			}
		}

	}
	////////////////////////////////////////////////////////////// BITMAP INDEX
	////////////////////////////////////////////////////////////// ////////////////////////////////////////////////////////////

	private ArrayList<Object> getDistinctValues(String tableName, String colName) {

		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + tableName + "\\PAGES.ser");
		TreeSet<Object> tree = new TreeSet<>();
		ArrayList<Object> distinctValues = new ArrayList<>();

		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + tableName + "\\Page" + PAGES.get(i) + ".ser");
			Vector<Hashtable<String, Object>> storage = e.getValues();
			for (int j = 0; j < storage.size(); j++) {
				Hashtable<String, Object> record = storage.get(i);
				Object o = record.get(colName);
				tree.add(o);
			}
		}

		for (Object insta : tree) {
			distinctValues.add(insta);
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
					koko = linee[i] + ",";
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
		String[] bitmaps = new String[distinct.size()];
		Arrays.fill(bitmaps, "");
		ArrayList<Integer> PAGES = (ArrayList<Integer>) deserializingAnObject(
				currentDir + "\\data\\" + strTableName + "\\PAGES.ser");
		for (int i = 0; i < PAGES.size(); i++) {
			Page e = (Page) deserializingAnObject(
					currentDir + "\\data\\" + strTableName + "\\Page" + PAGES.get(i) + ".ser");
			Vector<Hashtable<String, Object>> storage = e.getValues();
			for (int f = 0; i < distinct.size(); f++) {
				bitmaps[f] += bitmaps[f] + e.getPageName() + ",";
			}
			for (int j = 0; j < storage.size(); j++) {
				Hashtable<String, Object> record = storage.get(i);
				Object o = record.get(strColName);
				int index = distinct.indexOf(o);

				for (int k = 0; k < distinct.size(); k++) {
					if (k == index)
						bitmaps[k] = bitmaps[k] + "1";
					else
						bitmaps[k] = bitmaps[k] + "0";

				}

			}
			for (int f = 0; i < distinct.size(); f++) {
				bitmaps[f] += bitmaps[f] + ",";
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
		for (int j = 1; j <= (int) Math.ceil(bitmaps.length / max); j++) {
			Bitmap newBitmap = new Bitmap(strTableName, j, this, strColName);
			int i = 0;
			for (; i < (int) Math.min(max, bitmaps.length); i++) {
				ArrayList<Triple> bitmapOfValue = compressRLE(bitmaps[i]);
				Hashtable<Object, ArrayList<Triple>> bitmap = new Hashtable<>();
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
			String simplifiedForm = st.nextToken();
			char currentChar = simplifiedForm.charAt(0);
			int occurances = 1;
			for (int i = 1; i < simplifiedForm.length() - 1; i++) {

				if (currentChar != simplifiedForm.charAt(i)) {
					Triple p = new Triple(pageName, occurances, currentChar);
					compressForm.add(p);
					occurances = 1;
					currentChar = simplifiedForm.charAt(i);
				} else {
					occurances++;
				}
			}
			Triple p = new Triple(pageName, occurances, currentChar);
			compressForm.add(p);

		}

		return null;
	}


	public String decompressRLE(Triple p) {
		String decompressedForm = "";
		int numberOfVal = p.getNumberOfValue();
		for (int i = 0; i < numberOfVal; i++) {
			decompressedForm += p.getValue();
		}

		return decompressedForm;
	}
	
	public void deleteBitmapValue(String strTableName , Object val  ) { 
		/*
		 * you would mimic the delete of table one  
		 */
	}
	public void insertNewBitmapValue(String strTableName ){
/*
 * 
 * 
 * you would mimic the insert of table ; 		
 */
		
		
		
	}
	
	
	public void deleteValueBitmapIndex(String strTableName, String pageName, Object val, int position){
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().charAt(0) == 'B') {
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
								arr.remove(k) ; 
								int index = position - cumulative;
								String modified = "";
								if (val.toString().equals(key.toString())) {
									modified = decompressed.substring(0, index) + decompressed.substring(index+1);

								} else {
									modified = decompressed.substring(0, index) + decompressed.substring(index+1);

								}
								ArrayList<Triple> compressed = compressRLE(modified) ; 
								
								arr.addAll(k,compressed) ; 
							}
						}
					}

				}
				b.setAllBitmaps(allElements);
				serializingAnObject(b, currentDir + "\\data\\" + strTableName +"\\"+ b.getBitmapName());
			}
		}

	
		
	}
	public void insertValueBitmapIndex(String strTableName, String pageName, Object val, int position) {
		File folder = new File(currentDir + "\\data\\" + strTableName);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].getName().charAt(0) == 'B') {
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
								arr.remove(k) ; 
								int index = position - cumulative;
								String modified = "";
								if (val.toString().equals(key.toString())) {
									modified = decompressed.substring(0, index) + "1" + decompressed.substring(index);

								} else {
									modified = decompressed.substring(0, index) + "0" + decompressed.substring(index);

								}
								ArrayList<Triple> compressed = compressRLE(modified) ; 
								
								arr.addAll(k,compressed) ; 
							}
						}
					}

				}
				b.setAllBitmaps(allElements);
				serializingAnObject(b, currentDir + "\\data\\" + strTableName +"\\"+ b.getBitmapName());

			}
		}

	}

	static class Triple {
		private String pageName;
		private int numberOfValue;
		private int value;

		public Triple(String pageName, int numberValues, int val) {
			this.pageName = pageName;
			this.numberOfValue = numberValues;
			this.value = val;
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

	}

}
