import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

public class DBAppTest  {

	static String currentDirectory = System.getProperty("user.dir");

	public static void main(String[] args) throws DBAppException {

		try {

			DBApp a = new DBApp();

			String strTableName = "Student";
			Hashtable htblColNameType = new Hashtable();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			a.createTable(strTableName, "id", htblColNameType);

			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(4));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			a.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(2));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			a.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(1));
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("gpa", new Double(1.25));
			a.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(3));
			htblColNameValue.put("name", new String("John Noor"));
			htblColNameValue.put("gpa", new Double(1.5));
			a.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(5));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(0.88));
			a.insertIntoTable(strTableName, htblColNameValue);
			
			System.out.println("the table " );
			a.showContent("Student");
			System.out.println("lalalala");
			a.createBitmapIndex(strTableName, "name");
			Hashtable<String, Object> j = new Hashtable<String, Object>();
			j.put("name", "Bobobo");
			a.updateTable(strTableName, "3", j);
			a.showContent("Student");
			System.out.println("hahahah");
//			Hashtable<String, Object> jj = new Hashtable<String, Object>();
//			jj.put("gpa", 1.25);
			a.deleteFromTable("Student", j);
			System.out.println("after operations the table " );
			a.showContent("Student");
//			
//			Hashtable<String, Object> j = new Hashtable<String, Object>();
//			j.put("name", "yasser");
//			a.updateTable("Student", "4", j);
//
//			Hashtable<String, Object> jj = new Hashtable<String, Object>();
//	
//			SQLTerm p = new SQLTerm("Student", "name", "=", "Dalia Noor") ; 
//			Iterator<Hashtable<String  , Object >> c =  a.selectFromTable(new SQLTerm[]{p}, new String []{}) ;
//			while (c.hasNext()) { 
//				System.out.println("adasda");
//				System.out.println(c.next().toString());
//			}
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}


		
	
	}
}
