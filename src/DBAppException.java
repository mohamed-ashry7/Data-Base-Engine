
public class DBAppException extends Exception{
    String str1;
    
	DBAppException(String str2) {
		str1=str2;
	}
	
	public String toString(){ 
		return ("DBAppException Occurred: "+str1) ;
	}
}
