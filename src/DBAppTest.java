import java.io.File;
import java.util.Hashtable;

public class DBAppTest {

	
	
	
	
	public static void main(String[] args) throws DBAppException {
		String currentDir = System.getProperty("user.dir");
	        
//	        
//	    DBApp a = new DBApp() ; 
		String strTableName = "Student2";
//		Hashtable htblColNameType = new Hashtable( );
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.double");
//		a.createTable( strTableName, "id", htblColNameType );
		
		File  f = new File(currentDir + "\\" + strTableName);
		Boolean x = f.mkdirs();
		System.out.print(x);
	}
}
