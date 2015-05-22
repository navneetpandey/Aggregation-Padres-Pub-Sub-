package ca.utoronto.msrg.padres.demo.traffic;

public enum TrafficDataFileds {
	OCCU_PER(19),
	SPEED(21),
	NO_OF_VEHICLE(23);
	private final int id;
	TrafficDataFileds(int id) { this.id = id; }
    public int getValue() { return id; }
    
}
