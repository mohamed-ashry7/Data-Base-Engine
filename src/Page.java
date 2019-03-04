import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

public class Page implements Serializable {

	private static final int MAX_ROWS = 200;
	private int numberOfRows;
	private String tableName;
	private String pageName;
	private Vector<Hashtable<String, Object>> storage;
	private String clusteringType;
	private String clusteringValue;

	public Page(String strTableName, int number) {
		numberOfRows = 0;
		storage = new Vector<>();
		pageName = "Page" + number;
		tableName = strTableName;
	}
	public void printVector() { 
		System.out.println(storage);
	}
	public void addElement(Hashtable<String, Object> h) {
		System.out.println("size " + storage.size());

		boolean flag = true;
		for (int i = 0; i < storage.size(); i++) {
			if (h.get(clusteringValue).toString().compareTo(storage.get(i).get(clusteringValue).toString()) < 0) {
				storage.insertElementAt(h, i);
				flag = false;
				break;
			}
		}
		
		 if (flag )
		 storage.add(h) ;
		
		numberOfRows++;

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
		return numberOfRows == MAX_ROWS;
	}

	public boolean isEmpty() {
		return numberOfRows == 0;
	}

	public void updateRecord(String clusteredVal, Hashtable<String, Object> h) {

		Set keys = h.keySet();
		Iterator<String> it = keys.iterator();
		ArrayList<String> updatingValues = new ArrayList<>();
		while (it.hasNext()) {
			updatingValues.add(it.next());
		}
		for (int i = 0; i < storage.size(); i++) {

			Hashtable<String, Object> r = storage.get(i);

			if (clusteredVal.equals(r.get(clusteringValue).toString())) {

				for (int j = 0; j < updatingValues.size(); j++) {
			
					r.put(updatingValues.get(j), h.get(updatingValues.get(j)));
					r.put("TouchDate", new Date());
				}

			}

		}

	}

	
	public int removeRecord(Hashtable<String, Object> h) {

		Set keys = h.keySet();
		Iterator<String> it = keys.iterator();
		ArrayList<String> key = new ArrayList<>();
		while (it.hasNext()) {
			key.add(it.next());
		}
		int numberOfRemovedRecords = 0;
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
				storage.remove(r);
				numberOfRows-- ;
				numberOfRemovedRecords++;
			}
		}

		return numberOfRemovedRecords;
	}

}
