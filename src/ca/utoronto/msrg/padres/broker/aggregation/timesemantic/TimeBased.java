package ca.utoronto.msrg.padres.broker.aggregation.timesemantic;



public class TimeBased {

	public  TimeBased(  Notification notification) {
		this.notification = notification;
		
	}

	public enum Notification{
		SLIDING,
		TUMBLING,
		SEMANTIC
	}
	private Notification notification;

	public Notification getNotification() {
		return notification;
	}
	

}
