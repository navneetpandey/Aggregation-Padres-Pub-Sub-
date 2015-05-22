package ca.utoronto.msrg.padres.broker.aggregation.countsemantic;


public interface CountBasedAggregator {

	public void updateResult(int val);
	public int getCount();
	public void setCount(int count);
	public boolean checkTrigger ();
	public void reset();
	public void incrementCount();
	public String getResultString(); 
	
}
