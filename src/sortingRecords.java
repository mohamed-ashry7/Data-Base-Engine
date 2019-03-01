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
		switch (clusteringType) {
		case "java.lang.Integer":
			return ((Integer) val1).compareTo((Integer) val2)  ; 
		case "java.lang.Double":
			return ((Double) val1).compareTo((Double) val2)  ; 

		case "java.lang.String":
			return ((String) val1).compareTo((String) val2)  ; 

		case "java.lang.Boolean":
			return ((Boolean) val1).compareTo((Boolean) val2)  ; 

		case "java.util.Date":
			return ((Date) val1).compareTo((Date) val2)  ; 

		}

		return 0;
	}

}
