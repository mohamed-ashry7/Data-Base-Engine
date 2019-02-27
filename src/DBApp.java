import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

public class DBApp {

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		String tableName = strTableName;
		new File("C:\\Users\\Mohamed Elashry\\Software\\java-neon\\workspace\\DBProject\\" + tableName).mkdirs();

		Set keySet = htblColNameType.keySet();

		File metaData = new File("C:\\Users\\Mohamed Elashry\\Software\\java-neon\\workspace\\DBProject\\" + tableName
				+ "\\metadata.csv");
		File DATA = new File(
				"C:\\Users\\Mohamed Elashry\\Software\\java-neon\\workspace\\DBProject\\" + tableName + "\\DATA.txt");
		try {
			PrintWriter dataWriter = new PrintWriter(DATA.getPath());
			dataWriter.println("Page: " + 0);
			dataWriter.println("Rows: " + 0);
			dataWriter.println("ClusteringTable: " + strClusteringKeyColumn);
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

	private boolean createOrNot(String strTableName) {
		try {
			File tableFile = new File("C:\\Users\\Mohamed Elashry\\Software\\java-neon\\workspace\\DBProject\\"
					+ strTableName + "\\DATA");
			BufferedReader br = new BufferedReader(new FileReader(tableFile));
			br.readLine();
			int records = Integer.parseInt(br.readLine());
			return records % 200 == 0;
		} catch (IOException e) {
			return false;
		}
	}

	private int lastPage(String strTableName) {
		try {
			File tableFile = new File("C:\\Users\\Mohamed Elashry\\Software\\java-neon\\workspace\\DBProject\\"
					+ strTableName + "\\DATA");
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
			File tableFile = new File("C:\\Users\\Mohamed Elashry\\Software\\java-neon\\workspace\\DBProject\\"
					+ strTableName + "\\DATA");
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
			File tableFile = 
					new File("C:\\Users\\Mohamed Elashry\\Software\\java-neon\\workspace\\DBProject\\"
					+ strTableName + "\\DATA");
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

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Hashtable<String, Object> value = mapHash(htblColNameValue);
		int lastPage = lastPage(strTableName);
		String clusteringTable = clusteringColumn(strTableName);
		Page currentPage =null ; 
		if (lastPage == 0 || createOrNot(strTableName)) {
			lastPage++ ; 
			currentPage = new Page(strTableName , lastPage);
			increaseNoPages(strTableName);
		}
		else {
			// DESERIAZABLEEEEE 
		}
		

	}

}
