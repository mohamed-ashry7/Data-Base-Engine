import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import javax.swing.text.html.HTMLDocument.Iterator;
/*
 * you must check the methods 
 */
public class Bitmap implements Serializable {
	String currentDire = System.getProperty("user.dir");

	private int MAX_ROWS;
	private String tableName;
	private String BitmapName;
	private Vector<Hashtable<Object, ArrayList<DBApp.Triple>>> storage;
	private  DBApp theTable;

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

	


	
	public ArrayList<DBApp.Triple> getElementTriple (Object o  , int index) { 
		System.out.println("HAHAhohohomhmhmhhm");
		for (int i = 0 ; i < storage.size() ; i ++ ) { 
			System.out.println(storage.get(i).keys().nextElement().toString());
			System.out.println(storage.get(i).get(storage.get(i).keys().nextElement().toString()).toString());
		}
		return storage.get(index).get(o) ; 
	}
	public void addElement(Hashtable<Object, ArrayList<DBApp.Triple>>h) {

		Object value = h.keys().nextElement().toString(); 
		boolean flag = true ; 
		for (int i = 0 ; i < storage.size() ; i ++ ) { 
			if (value.toString().compareTo(storage.get(i).keys().nextElement().toString())<0) { 
				storage.insertElementAt(h, i);

				flag = false;
				break;
				
			}
		}
//		System.out.println("HAHAHHAHAHAHAHHAHAH");
//		for (int i = 0 ; i < storage.size() ; i ++ ) { 
//			System.out.println(storage.get(i).keys().nextElement().toString());
//		}
		if (flag)
			storage.add(h);
	}
	
	public Vector<Hashtable<Object , ArrayList<DBApp.Triple>>> getAllBitmaps () { 
		return storage ; 
	}
	public void setAllBitmaps (Vector store) { 
		this.storage = store ;  
	}
	public Object getLastElementKey() {
		return storage.get(storage.size() - 1).keys().nextElement() ;
	}

	public Hashtable<Object, ArrayList<DBApp.Triple>> removeLastElement() {
		return storage.remove(storage.size() - 1) ;
	}
	
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

	public boolean bitmapContainsKey(Object a ) { 

		for (int i = 0 ; i < storage.size() ; i ++ ) {
			if (a.toString().equals(storage.get(i).keys().nextElement().toString())){
				return true ; 
			}
		}
		return false ; 
	}
//	public void updateRecord(Object preValue, Object newVal) {
//		
//	}

	public boolean  removeRecord(Object h) {
			
			
			for (int i = 0 ; i <storage.size() ; i ++ ) { 
				if (h.toString().equals(storage.get(i).keys().nextElement().toString())){
					storage.remove(i) ; 
					return true ;  

				}
			}
		return false ; 
	}

}
