package ca.utoronto.msrg.padres.broker.aggregation.aggregationcore;

import ca.utoronto.msrg.padres.broker.aggregation.operator.DecomposableOperator;
import ca.utoronto.msrg.padres.broker.aggregation.operator.IndirectDecomposableOperator;
import ca.utoronto.msrg.padres.broker.aggregation.operator.NonDecomposableOperator;
import ca.utoronto.msrg.padres.broker.aggregation.operator.Operator;
import ca.utoronto.msrg.padres.broker.aggregation.operator.OperatorType;

public class AggregateComputer {
	

	public AggregationType type;
	protected OperatorType operationType;
	protected boolean resultExist = false;
	Operator operator;


	
	public AggregateComputer(OperatorType operatorType, AggregationType type){
		this.operationType = operatorType;
		this.type = type;

		switch(operatorType.getAggregationComputationType()){
		case NONDECOMPOSABLE:
			this.operator=new NonDecomposableOperator(operatorType);
			break;
		case DECOMPOSABLE:
			this.operator=new DecomposableOperator(operatorType);
			break;
		case INDIRECTDECOMPOSABLE:
			this.operator=new IndirectDecomposableOperator(operatorType);
			break;
		}
		
		
		
		
	}
	
	

	public void aggregateComputation(String sValue){
		operator.aggregateComputation(sValue);
	}
	
	
	
	
	

	/*
	public boolean samePredicateMap( Map<String, Predicate> iPredMap ){
		if(iPredMap.size() == predMap.size()) {
			Iterator<Entry<String, Predicate>> entries = iPredMap.entrySet().iterator();
			while (entries.hasNext()) {
				Entry<String, Predicate> entry = entries.next();
				String key = entry.getKey();
				  if(!(this.predMap.containsKey(key) && samePredicate(this.predMap.get(key),entry.getValue()))){
					return false;			  
				  }
            }
			return true;
		}
		
		
		return false;
	}
	
	
	
	
	*/
	
	
	public OperatorType getOperator(){
		return operationType;
	}
	
	
	
	
	public boolean resultExist(){
		return operator.resultExist();
	}

	public String getResultString(boolean forClient){
		return operator.getResultString(forClient);
	}
	
	public void reset() {
		operator.reset();
	}
	
}
