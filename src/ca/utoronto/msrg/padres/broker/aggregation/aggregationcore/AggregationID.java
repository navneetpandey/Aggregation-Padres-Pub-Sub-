package ca.utoronto.msrg.padres.broker.aggregation.aggregationcore;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import ca.utoronto.msrg.padres.broker.aggregation.operator.OperatorType;
import ca.utoronto.msrg.padres.common.message.Predicate;

public class AggregationID implements Serializable{

	Map<String, Predicate> predMap;
	long windowSize;
	long shiftSize;
	String fieldName;
	public long aggregationID;
	OperatorType operatorType;
	String subID;

	public String getSubID() {
		return subID;
	}



	public void setSubID(String subID) {
		this.subID = subID;
	}



	public AggregationID(Map<String, Predicate> predMap, long windowSize,
			long shiftSize, String fieldname,OperatorType operatorType, String brokerID) {
		super();
		this.predMap = predMap;
		this.windowSize = windowSize;
		this.shiftSize = shiftSize;
		this.fieldName = fieldname;
		this.operatorType=operatorType;
		this.subID=brokerID;
		aggregationID = AggregationIDGenerator.generateAggSubID(predMap,
				windowSize, shiftSize);
	}

	
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof AggregationID))return false;
		return aggregationID==((AggregationID)obj).aggregationID;// && subID.equals(((AggregationID)obj).subID);
	}

	
	


	@Override
	public int hashCode() {
		return (int) (aggregationID%Integer.MAX_VALUE)/*+subID.hashCode()*/;
	}



	public Map<String, Predicate> getPredMap() {
		return predMap;
	}

	public void setPredMap(Map<String, Predicate> predMap) {
		this.predMap = predMap;
	}

	public long getWindowSize() {
		return windowSize;
	}

	public void setWindowSize(long windowSize) {
		this.windowSize = windowSize;
	}

	public long getShiftSize() {
		return shiftSize;
	}

	public void setShiftSize(long shiftSize) {
		this.shiftSize = shiftSize;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldname) {
		this.fieldName = fieldname;
	}

/*	public long getAggregationIDInLong() {
		return aggregationID;
	}
*/
	public OperatorType getOperatorType() {
		return operatorType;
	}

	public boolean containsAllPredicate(Map<String, Predicate> iPredMap) {

		if (iPredMap.size() > 0) {

			Iterator<Entry<String, Predicate>> entries = predMap.entrySet()
					.iterator();
			while (entries.hasNext()) {
				Entry<String, Predicate> entry = entries.next();
				String key = entry.getKey();
				if (!(iPredMap.containsKey(key) && samePredicate(
						iPredMap.get(key), entry.getValue()))) {
					return false;
				}
			}
			return true;

		} else
			return false;
	}

	private boolean samePredicate(Predicate p1, Predicate p2) {
		if (p1.getOp().equals(p2.getOp())
				&& (p1.getValue().equals(p2.getValue()))) {
			return true;
		} else {
			return false;
		}
	}

}
