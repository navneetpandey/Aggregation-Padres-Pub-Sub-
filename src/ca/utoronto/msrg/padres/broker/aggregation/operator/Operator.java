package ca.utoronto.msrg.padres.broker.aggregation.operator;

public interface Operator {

	public void aggregateComputation(String sValue);

	public void reset();

	public boolean resultExist();
	
	public String getResultString(boolean forClient);
	
}
