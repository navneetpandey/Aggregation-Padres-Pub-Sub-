package ca.utoronto.msrg.padres.broker.aggregation.aggregationcore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import ca.utoronto.msrg.padres.common.message.Predicate;

public class AggregationIDGenerator {
 
	
	public static long generateAggSubID(Map<String, Predicate> predMap , long windowSize, long shiftSize) {
		ArrayList<String> keySet = new ArrayList<String>(predMap.keySet());
		
		Collections.sort(keySet);
		
		StringBuffer id = new StringBuffer();
		
		for(String key : keySet) {
			id.append(key);
			id.append(predMap.get(key).getOp());
			id.append(predMap.get(key).getValue().toString());
		}
		
		id.append(""+windowSize);
		id.append(""+shiftSize);
		
		if(id.length() == 0)
			return -1;
		else
			return PJWHash(id.toString());
	}
	
	static boolean matchAggIDWithPredicates(long keyToBeMatched, Map<String, Predicate> predMap, long windowSize, long shiftSize){
		if(keyToBeMatched == generateAggSubID( predMap, windowSize,  shiftSize))
			return true;
		else
			return false;
	}
	

	 private static long PJWHash(String str)
	   {
	      long BitsInUnsignedInt = (long)(4 * 8);
	      long ThreeQuarters     = (long)((BitsInUnsignedInt  * 3) / 4);
	      long OneEighth         = (long)(BitsInUnsignedInt / 8);
	      long HighBits          = (long)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
	      long hash              = 0;
	      long test              = 0;

	      for(int i = 0; i < str.length(); i++)
	      {
	         hash = (hash << OneEighth) + str.charAt(i);

	         if((test = hash & HighBits)  != 0)
	         {
	            hash = (( hash ^ (test >> ThreeQuarters)) & (~HighBits));
	         }
	      }

	      return hash;
	   }
	
}
