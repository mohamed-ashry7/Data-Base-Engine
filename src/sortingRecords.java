import java.io.Serializable;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;

public class sortingRecords implements Comparator<Hashtable<String, Object>> {
	private String clusteringType;
	private String clusteringValue;

	public void setClustering(String value, String type) {
		clusteringValue = value;
		clusteringType = type;
	}

	public int compare(Hashtable<String, Object> hash1, Hashtable<String, Object> hash2) {

		Object val1 = hash1.get(clusteringValue ) ;
		Object val2 = hash2.get(clusteringValue) ; 
		return val1.toString().compareTo(val2.toString()) ; 

		

	
	}

}
