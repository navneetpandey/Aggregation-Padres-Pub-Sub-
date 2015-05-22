package ca.utoronto.msrg.padres.broker.aggregation.countsemantic;

import java.util.ArrayList;


//////////////////////DS for maintaining Decomposable ////////////////////////
class NonDecompoAggregationValueCB implements CountBasedAggregator{
	private ArrayList<Integer> partialResult;
	public ArrayList<Integer> getPartialResult() {
		return partialResult;
	}
	public String getResultString(){
		String s = "";
		for (Integer result : partialResult) {   
			s += (result.toString() + "_");
		}
		return s;
	}
	public void setPartialResult(ArrayList<Integer> partialResult) {
		this.partialResult = partialResult;
	}
	public void addResult(Integer result) {
		this.partialResult.add(result);
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}

	int count;
	int triggerCond;

	public boolean checkTrigger ()
	{
		return this.count > this.triggerCond;
	}
	public NonDecompoAggregationValueCB()
	{
		partialResult = new ArrayList<Integer>();
		count = 0;
		triggerCond = 2;
	}
	
	public NonDecompoAggregationValueCB(int count, int triggerCond) {
		super();
		partialResult = new ArrayList<Integer>();
		this.count = count;
		this.triggerCond = triggerCond;
	}
	
	public void reset()
	{
		partialResult.clear();
		count = 0;
		triggerCond = 2;
		
	}
	public void incrementCount() {
		count++;
		
	}
	
	public void updateResult(int val)
	{
		addResult(val);
	}
}