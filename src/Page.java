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

import javax.annotation.processing.ProcessingEnvironment;
import javax.swing.text.html.HTMLDocument.HTMLReader.PreAction;

public class Page implements Serializable {
	String currentDire = System.getProperty("user.dir");

	private int MAX_ROWS;
	private String tableName;
	private String pageName;
	private Vector<Hashtable<String, Object>> storage;
	private String clusteringType;
	private String clusteringValue;
	private DBApp theTable;

	public Page(String strTableName, int number, DBApp table) {
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(currentDire + "\\config\\config.properties"));
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		String value = properties.getProperty("maxNumberRows");
		MAX_ROWS = Integer.parseInt(value);

		storage = new Vector<>();
		pageName = "Page" + number;
		tableName = strTableName;
		theTable = table;
	}

	public Vector<Hashtable<String, Object>> getValues() {

		return storage;
	}

	public void printVector() {
		for (int i = 0; i < storage.size(); i++) {
			System.out.println(storage.get(i));
		}

	}

	public int addElement(Hashtable<String, Object> h) {
		System.out.println("size " + storage.size());

		boolean flag = true;
		int i = 0;
		for (; i < storage.size(); i++) {
			if (h.get(clusteringValue).toString().compareTo(storage.get(i).get(clusteringValue).toString()) < 0) {
				storage.insertElementAt(h, i);
				flag = false;
				break;
			}
		}

		if (flag)
			storage.add(h);
		return i;

	}

	public void setClustering(String value, String type) {
		clusteringType = type;
		clusteringValue = value;
	}

	public Hashtable<String, Object> getLastElement() {
		return storage.get(storage.size() - 1);
	}

	public Hashtable<String, Object> removeLastElement() {
		return storage.remove(storage.size() - 1);
	}

	public String getPageName() {
		return pageName;
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

	public ArrayList<Pair> updateRecord(String clusteredVal, Hashtable<String, Object> h) {
		ArrayList<Pair> positions = new ArrayList<>();
		Set keys = h.keySet();
		Iterator<String> it = keys.iterator();
		ArrayList<String> updatingValues = new ArrayList<>();
		boolean isID = false;
		while (it.hasNext()) {
			String k = it.next();
			updatingValues.add(k);
			if (k.equals(clusteringValue)) {
				isID = true;
			}
		}
		for (int i = 0; i < storage.size(); i++) {

			Hashtable<String, Object> r = storage.get(i);

			if (clusteredVal.equals(r.get(clusteringValue).toString())) {

				positions.add(new Pair (i, r ));
				for (int j = 0; j < updatingValues.size(); j++) {

					r.put(updatingValues.get(j), h.get(updatingValues.get(j)));
					r.put("TouchDate", new Date());

				}
				/*
				 * if (isID) { storage.remove(i);
				 * 
				 * try {
				 * 
				 * theTable.insertIntoTable(tableName, r);
				 * 
				 * } catch (DBAppException e) { e.printStackTrace(); }
				 */
			}

		}
		return positions;
	}

	public ArrayList<Integer> removeRecord(Hashtable<String, Object> h) {
		ArrayList<Integer> positions = new ArrayList<>();
		Set keys = h.keySet();
		Iterator<String> it = keys.iterator();
		ArrayList<String> key = new ArrayList<>();
		while (it.hasNext()) {
			String k = it.next();
			key.add(k);
		}
		this.printVector();

		for (int i = 0; i < storage.size(); i++) {

			Hashtable<String, Object> r = storage.get(i);
			int counter = 0;
			for (int j = 0; j < key.size(); j++) {

				String currentType = key.get(j);

				if (h.get(currentType).toString().equals(r.get(currentType).toString())) {
					counter++;
				} else {
					break;
				}

			}
			if (counter == h.size()) {
				positions.add(i);
				storage.remove(i);
				i--;
			}
		}

		return positions;
	}

	static class Pair implements Serializable {
		Hashtable<String, Object> previousVal;
		int pos;
		
		public Pair (int pos ,Hashtable<String , Object > p  ) { 
			
			this.pos = pos ; 
			this.previousVal  = p ; 
		}
		
		public Hashtable<String, Object> getPreviousVal() {
			return previousVal;
		}

		public void setPreviousVal(Hashtable<String, Object> previousVal) {
			this.previousVal = previousVal;
		}

		public int getPos() {
			return pos;
		}

		public void setPos(int pos) {
			this.pos = pos;
		}
	}

}


