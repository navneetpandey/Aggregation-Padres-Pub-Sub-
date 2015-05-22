package ca.utoronto.msrg.padres.broker.aggregation.aggregationcore;

public enum AggregationMode {
	RAW_MODE,
	AGG_MODE,
	TRANSIT_MODE;//only for aggregation engine. It could be better to make some constrain 
	//which could avoid assigning this to aggregators.
}


