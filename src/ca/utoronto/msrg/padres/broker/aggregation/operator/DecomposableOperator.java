package ca.utoronto.msrg.padres.broker.aggregation.operator;

public class DecomposableOperator implements Operator {

	protected int partialResult;
	boolean resultExist;
	OperatorType operatorType;

	public DecomposableOperator(OperatorType operatorType) {
		this.operatorType = operatorType;
		reset();
	}

	@Override
	public void aggregateComputation(String sValue) {
		int val = Integer.parseInt(sValue);
		switch (operatorType) {
		case MAX:
			if (val > partialResult)
				partialResult = val;
			break;
		case MIN:
			if (val < partialResult)
				partialResult = val;
			break;
		case SUM:
			partialResult += val;
			break;
		case COUNT:
			partialResult++;
			break;
		default:
			break;

		}
		// we got some result
		resultExist = true;
	}

	@Override
	public void reset() {

		switch (operatorType) {
		case MIN:
			partialResult = Integer.MAX_VALUE;
			break;

		default:
			partialResult = 0;
			break;

		}
		resultExist = false;

	}

	@Override
	public String getResultString(boolean forClient) {
		// convert result from PubVector
		String s = Integer.toString(partialResult);
		return s;
	}

	public boolean resultExist() {
		return resultExist;
	}

}
