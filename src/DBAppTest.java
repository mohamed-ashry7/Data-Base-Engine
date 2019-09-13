import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;

public class DBAppTest {

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
			htblColNameValue.put("gpa", new Double(0.88));
			a.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(5));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(0.88));
			a.insertIntoTable(strTableName, htblColNameValue);

			System.out.println("the table ");
			a.showContent("Student");
			// a.createBitmapIndex(strTableName, "name");
//			Hashtable<String, Object> j = new Hashtable<String, Object>();
//			j.put("name", "Ashry");
//			a.updateTable(strTableName, "3", j);
//			a.showContent("Student");
//			System.out.println();
//			a.showContent("Student");
			a.createBitmapIndex(strTableName, "name");
			a.showBitmap(strTableName, "name");
			
//			a.showBitmap("Student", "name");
//			a.deleteFromTable(strTableName, j);
//			System.out.println();
//			a.showBitmap("Student", "name");
//			System.out.println("HEEEEEEEEEEEEEEEEEEEEEERRRRRRRRRRRRRRRRRRREEEEEEEEEEEEEEEEE");
//			a.showContent(strTableName);
			Hashtable<String, Object> jj = new Hashtable<String, Object>();
			jj.put("name", "John Noor");
//			jj.put("gpa", 0.12);
//			jj.put("id", 9);
//			a.insertIntoTable(strTableName, jj);
			a.deleteFromTable(strTableName, jj);
			a.updateTable(strTableName, "2", htblColNameValue);
			a.showBitmap(strTableName, "name");
//			jj.put("name", "okokokokok") ; 
//			a.updateTable(strTableName, "9", jj);
//			a.showContent(strTableName);
			// a.showContent("Student");
			// System.out.println("hahahah");
			// Hashtable<String, Object> jj = new Hashtable<String, Object>();
			// jj.put("gpa", 1.25);
			// a.deleteFromTable("Student", j);
			// System.out.println("after operations the table " );
			// a.showContent("Student");
			//
			// Hashtable<String, Object> j = new Hashtable<String, Object>();
			// j.put("name", "yasser");
			// a.updateTable("Student", "4", j);
			//
			// Hashtable<String, Object> jj = new Hashtable<String, Object>();
			//
			System.out.println();
			 SQLTerm p = new SQLTerm("Student", "name", "=", "Dalia Noor") ;
//			 SQLTerm pp = new SQLTerm("Student", "name", "=", "Ahmed Noor" ) ;
//			 SQLTerm ppp = new SQLTerm("Student", "id", ">", 3) ;
			
			 Iterator<Hashtable<String , Object >> c = a.selectFromTable(new
			 SQLTerm[]{p}, new String []{}) ;
			 while (c.hasNext()) {
			 System.out.println(c.next().toString());
			 }
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}

	}
}
