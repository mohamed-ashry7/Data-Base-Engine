import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Hashtable;

public class Page implements java.io.Serializable {

	private static final int MAX_ROWS = 200;
	private int currentRows;
	private Page next;
	private String tableName ; 
	private String pageName ; 
	private ArrayList<Hashtable<String,Object >> storage ;
	
	
	public Page(String strTableName , int number ) {
		currentRows = 0;
		storage = new ArrayList<>(MAX_ROWS) ;
		pageName = "Page" + number ; 
		tableName = strTableName ; 
		
	}

	public Page getNext() {
		return next;
	}

	public void setNext(Page e) {
		next = e;
	}
	public void addElement(Hashtable<String , Object> h ) { 
		
		currentRows++ ; 
		
	}
	public Hashtable<String , Object> getLastElement () { 
		return storage.get(storage.size()-1) ; 
	}
	public String getPageName () { 
		return pageName ; 
	}
	public boolean isFull () { 
		return currentRows==MAX_ROWS ; 
	}

}
