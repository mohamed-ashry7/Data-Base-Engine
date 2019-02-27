import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Hashtable;

public class Page implements java.io.Serializable {

	private static final int MAX_ROWS = 200;
	private int currentRows;
	private Page next;
	private ArrayList<Hashtable<String,Object >> storage ; 
	public Page(String strTableName , int number ) {
		currentRows = 0;
		storage = new ArrayList<>(MAX_ROWS) ;
		String str= "Page" + number+".ser" ; 
		File newPage = new File("C:\\Users\\Mohamed Elashry\\Software\\java-neon\\workspace\\DBProject\\"
				+ strTableName + "\\"+str);
	}

	public Page getNext() {
		return next;
	}

	public void setNext(Page e) {
		next = e;
	}
	public void addElement(Hashtable<String , Object> h ) { 
		
	}

}
