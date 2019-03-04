import java.util.Hashtable;
import java.util.Vector;

public class DBAppTest {

	
	
	
	
	public static void main(String[] args) throws DBAppException {
		DBApp a = new DBApp() ; 
		
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		a.createTable( strTableName, "id", htblColNameType );
		
		
		Hashtable htblColNameValue = new Hashtable( );
		htblColNameValue.put("id", new Integer( 4));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		a.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 2 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		a.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 1));
		htblColNameValue.put("name", new String("Dalia Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.25 ) );
		a.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 3 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		a.insertIntoTable( strTableName , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 5 ));
		htblColNameValue.put("name", new String("Zaky Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.88 ) );
		a.insertIntoTable( strTableName , htblColNameValue );
		Hashtable<String, Object> j = new  Hashtable<String,Object>() ; 
		j.put("id", new Integer(4)) ; 
		Hashtable<String, Object> h = new  Hashtable<String,Object>() ; 
		h.put("id", new Integer(5)) ; 
		a.deleteFromTable("Student", j);
		a.deleteFromTable("Student", h);
		
		a.showContent();
	}
}
