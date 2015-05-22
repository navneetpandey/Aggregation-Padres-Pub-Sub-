package ca.utoronto.msrg.padres.broker.aggregation.operator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class NonDecomposableOperator  implements  Operator{

	boolean resultExist;
	OperatorType operatorType;
	
	public NonDecomposableOperator(OperatorType operatorType) {
		this.operatorType=operatorType;
		items = new HashSet<String>();
		reset();
	}

	Set<String> items;
	@Override
	public void aggregateComputation(String sValue) {
		switch(operatorType) {
		case DISTINCT:
			String delims = "-";
			String[] tokens = sValue.split(delims);
			for( int i=0; i<tokens.length;i++){		
				items.add(tokens[i]);
			}
			break;

		default:
			break;

		}
		//we got some result
		resultExist =true;
	}

	@Override
	public void reset() {

		switch(operatorType) {
		case DISTINCT:
			if(!items.equals(null))
				items.clear();
			break;

		default:
			if(!items.equals(null))
				items.clear();
			break;

		}
		resultExist = false;

	}

	@Override
	public String getResultString(boolean forClient) {
		String s = null;
		if(forClient) 
			s = Integer.toString(items.size());
		else{
			Iterator<String> it = items.iterator();
			boolean first = true;
			while (it.hasNext()) {
				if(first) {
					s = it.next();
					first = false;
				}
				else
					s = s + "-" +  it.next();
			}
		}

		return s;
	}

	

	public boolean resultExist(){
		return resultExist;

	}

}
