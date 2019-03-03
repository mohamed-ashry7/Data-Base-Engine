
public class DBAppException extends Exception{
    
	 public DBAppException(String message) {
		super(message) ; 
	}
	
	public String toString(){ 
		return ("DBAppException Occurred: "+ super.getMessage()) ;
	}
}
