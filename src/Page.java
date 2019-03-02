import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

public class Page implements java.io.Serializable {

	private static final int MAX_ROWS = 200;
	private int numberOfRows;
	private Page next;
	private String tableName ; 
	private String pageName ; 
	private Vector<Hashtable<String,Object >> storage ;
	private sortingRecords sort ; 
	public Page(String strTableName , int number ) {
		numberOfRows = 0;
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
		numberOfRows++ ; 
		
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
		return numberOfRows==MAX_ROWS ; 
	}
	public boolean isEmpty () {
		return numberOfRows ==0 ; 
	}
	
	
	
	public void removeRecord(Hashtable<String , Object > h ){
		
		Set keys =h.keySet() ; 
		Iterator<String> it = keys.iterator();
		ArrayList<String> key = new ArrayList<> () ; 
		while (it.hasNext()) { 
			key.add(it.next()) ; 
		}
		for (int i = 0 ; i < storage.size() ; i ++) { 
			
			Hashtable<String , Object > r = storage.get(i) ; 
			int counter = 0 ; 
			for (int j = 0 ; j < key.size() ; j ++  ) { 
				
				String type = key.get(j) ; 
				switch (type) {
				case "java.lang.Integer":
					 if(((Integer)r.get(type) ).intValue() == ((Integer)h.get(type)).intValue())
						 counter ++ ; break ; 
				case "java.lang.Double":
					 if(((Double)r.get(type) ).doubleValue() == ((Double)h.get(type)).doubleValue())
						 counter ++ ; break ;
				case "java.lang.String":
					 if(((String)r.get(type) ).equals(((String)h.get(type))))
						 counter ++ ; break ;
				case "java.lang.Boolean":
					 if(((Boolean)r.get(type) ).booleanValue() == ((Boolean)h.get(type)).booleanValue())
						 counter ++ ; break ;
				case "java.util.Date":
					 if(((Date)r.get(type) ).compareTo(((Date)h.get(type))) ==  0)
						 counter ++ ; break ;
				}
				
			}
			if (counter == h.size())
				storage.remove(r) ; 
		}
		
		
	}
	
	

}