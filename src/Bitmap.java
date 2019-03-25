import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class Bitmap implements Serializable {
	String currentDire = System.getProperty("user.dir");

	private int MAX_ROWS;
	private String tableName;
	private String BitmapName;
	private Vector<Hashtable<Object, ArrayList<DBApp.Triple>>> storage;
	private DBApp theTable;

	public Bitmap(String strTableName, int number, DBApp table ,String column ) {
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(currentDire + "\\config\\config.properties"));
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		String value = properties.getProperty("maxNumberRows");
		MAX_ROWS = Integer.parseInt(value);
		storage = new Vector<>();
		BitmapName = "Bitmap"+column + number;
		tableName = strTableName;
		theTable = table;
	}

	

	

	public void addElement(Hashtable<Object, ArrayList<DBApp.Triple>>h) {
		

	}


//	public Hashtable<String, Object> getLastElement() {
//		return storage.get(storage.size() - 1);
//	}
//
//	public Hashtable<String, Object> removeLastElement() {
//		return storage.remove(storage.size() - 1);
//	}

	public String getBitmapName() {
		return BitmapName;
	}

	public boolean isFull() {
		return storage.size() == MAX_ROWS;
	}

	public boolean isEmpty() {
		return storage.size() == 0;
	}

	public int getNumberOfRows() {
		return storage.size();
	}

	public void updateRecord(String clusteredVal, Hashtable<String, Object> h) {}

	public int removeRecord(Hashtable<String, Object> h) {
		return 0 ; 
	}

}
