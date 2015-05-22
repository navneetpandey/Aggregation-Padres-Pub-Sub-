package ca.utoronto.msrg.padres.broker.aggregation.adaptation;


public class AdaptationController implements Runnable{

	AdaptationSwitch adapSwitch;
	long adaptationFrequency;


	public AdaptationController(AdaptationSwitch adapSwitch,long adaptationFrequency) {
		super();
		this.adapSwitch = adapSwitch;
		this.adaptationFrequency = adaptationFrequency;
	}

	public void run() {

		while(true) {
			try {
				Thread.sleep(adaptationFrequency*adapSwitch.getSampleWindowSize());
			} catch (InterruptedException e) { 
				e.printStackTrace();
			}
			adapSwitch.checkForAggregationTrigger();
		}


	}

}
