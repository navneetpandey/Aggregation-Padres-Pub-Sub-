package ca.utoronto.msrg.padres.broker.aggregation.operator;

import ca.utoronto.msrg.padres.broker.aggregation.AggregationComputation;


public enum OperatorType{
	MAX (AggregationComputation.DECOMPOSABLE),
	MIN (AggregationComputation.DECOMPOSABLE),
	AVG (AggregationComputation.INDIRECTDECOMPOSABLE),
	SUM (AggregationComputation.DECOMPOSABLE),
	COUNT (AggregationComputation.DECOMPOSABLE),
	DISTINCT (AggregationComputation.NONDECOMPOSABLE),
	MOD (AggregationComputation.NONDECOMPOSABLE),
	MEDIAN(AggregationComputation.NONDECOMPOSABLE),
	RANGE(AggregationComputation.INDIRECTDECOMPOSABLE),
	SET(AggregationComputation.NONDECOMPOSABLE),
	UNDEF(AggregationComputation.DECOMPOSABLE);

	private final AggregationComputation aggregationComputation;
	private OperatorType(final AggregationComputation aggregationComputation) { this.aggregationComputation = aggregationComputation; }

	public static OperatorType fromString(String s){
		if(s.equals("MAX") || s.equals("max"))
			return MAX;
		else if( s.equals("MIN") || s.equals("min"))
			return MIN;
		else if ( s.equals("AVG") || s.equals("avg" ))
			return AVG;
		else if ( s.equals("SUM") || s.equals("sum"))
			return SUM;
		else if ( s.equals("COUNT") || s.equals("count"))
			return COUNT;
		else if ( s.equals("DISTINCT") || s.equals("distinct"))
			return DISTINCT;
		else if ( s.equals("MOD") || s.equals("mod"))
			return MOD;
		else if ( s.equals("MEDIAN") || s.equals("median"))
			return MEDIAN;
		else if ( s.equals("RANGE") || s.equals("range"))
			return RANGE;
		else if ( s.equals("SET") || s.equals("set"))
			return SET;
		else 
			return UNDEF;

	}
	
	public AggregationComputation getAggregationComputationType() {
		return aggregationComputation;
	}

	public  String convertString(){

		switch(this){
		case MAX :return "MAX"; 
		case MIN :return "MIN"; 
		case AVG :return "AVG"; 
		case SUM :return "SUM"; 
		case COUNT :return "COUNT"; 
		case DISTINCT :return "DISTINCT"; 
		case MOD :return "MOD"; 
		case MEDIAN :return "MEDIAN"; 
		case RANGE :return "RANGE"; 
		case SET :return "SET"; 
		default: return "UNDEF"; 

		}


	}

	public AggregationComputation getComputationType(){
		return aggregationComputation;
	}
}
