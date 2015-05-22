package ca.utoronto.msrg.padres.broker.aggregation.operator;


public class IndirectDecomposableOperator implements Operator{
	
	
	boolean resultExist;
	OperatorType operatorType;
	

	public IndirectDecomposableOperator(OperatorType operatorType) {
		this.operatorType=operatorType;
		reset();
	}

	private int min;
	private int max;
	@Override
	public void aggregateComputation(String sValue) {
		int imax = 0;
		int imin = 0;
		String delims = "-";
		String[] tokens = sValue.split(delims);
		imin = Integer.parseInt(tokens[0]);
		if(tokens.length == 2) {		
			imax = Integer.parseInt(tokens[1]);
		}
		else {
			imax=imin;
		}

		switch(operatorType) {
		case RANGE:
			if(imax> max) 
				max=imax;
			if(imin < min) 
				min=imin;
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
		case RANGE:
			min = Integer.MAX_VALUE;
			max = Integer.MIN_VALUE;
			break;

		default:
			min =0;
			max =0;
			break;

		}
		resultExist = false;

	}

	@Override
	public String getResultString(boolean forClient) {
		String s = null;
		if(forClient) 
			s = Integer.toString(max-min);
		else
			s = Integer.toString(min) + "-" + Integer.toString(max);
		return s;
	}


	
	public boolean resultExist(){
		return resultExist;

	}

}
