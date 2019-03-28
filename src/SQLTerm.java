import java.util.ArrayList;

public class SQLTerm {
	public String _strTableName;
	public String _strColumnName;
	public String _strOperator;
	public Object _objValue;
	
	public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue) {
		super();
		this._strTableName = _strTableName;
		this._strColumnName = _strColumnName;
		this._strOperator = _strOperator;
		this._objValue = _objValue;
	}
	
	public void isValidOperator() throws DBAppException { 
		ArrayList<String > operators = new ArrayList<>() ; 
		operators.add(">") ; 
		operators.add("<") ; 
		operators.add(">=") ; 
		operators.add("<=") ; 
		operators.add("!=") ; 
		operators.add("=") ; 

		if (!operators.contains(this._strOperator)) { 
			throw new DBAppException("INVALID OPERATOR") ; 
		}
	}

	

}
