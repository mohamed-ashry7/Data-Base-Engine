import java.util.Hashtable;

public class DBAppTest {

	
	
	
	
	public static void main(String[] args) throws DBAppException {
		DBApp a = new DBApp() ; 
		
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		a.createTable( strTableName, "id", htblColNameType );
	}
}
