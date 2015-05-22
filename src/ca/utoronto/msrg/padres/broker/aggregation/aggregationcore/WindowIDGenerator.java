package ca.utoronto.msrg.padres.broker.aggregation.aggregationcore;

public class WindowIDGenerator {
 
	public static String generateID(long time, String brokerID, long agSubID) {
		
		return ""+time+","+brokerID+","+agSubID;
	}
	
	public boolean isMatching(String key1, String key2) {
		if(key1==null)
			return false;
		return key1.equals(key2);
	}
 
}
