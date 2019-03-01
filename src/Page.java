import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements java.io.Serializable {

	private static final int MAX_ROWS = 200;
	private int currentRows;
	private Page next;
	private String tableName ; 
	private String pageName ; 
	private Vector<Hashtable<String,Object >> storage ;
	private sortingRecords sort ; 
	public Page(String strTableName , int number ) {
		currentRows = 0;
		storage = new Vector<>(MAX_ROWS) ;
		pageName = "Page" + number ; 
		tableName = strTableName ; 
		sort = new sortingRecords(); 
	}

	public Page getNext() {
		return next;
	}

	public void setNext(Page e) {
		next = e;
	}
	public void addElement(Hashtable<String , Object> h ) { 
		this.storage.add(h) ; 
		Collections.sort(storage, sort);
		currentRows++ ; 
		
	}
	public void setClustering (String value , String type ) { 

		sort.setClustering(value , type);
	}
	public Hashtable<String , Object> getLastElement () { 
		return storage.get(storage.size()-1) ; 
	}
	
	public Hashtable<String , Object> removeLastElement () { 
		return storage.remove(storage.size()-1) ; 
	}
	public String getPageName () { 
		return pageName ; 
	}
	public boolean isFull () { 
		return currentRows==MAX_ROWS ; 
	}
	
	

}
