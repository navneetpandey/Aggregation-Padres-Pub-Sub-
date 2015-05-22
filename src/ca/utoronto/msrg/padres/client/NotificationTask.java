package ca.utoronto.msrg.padres.client;

import java.util.Timer;
import java.util.TimerTask;

import ca.utoronto.msrg.padres.common.message.Publication;


public class NotificationTask extends TimerTask {

	private int maxFrequency;
	Timer notifier;
	Client client;
	int count;
	Publication newPub;
	 

	public NotificationTask(Timer notifier, Client cl,Publication newPub) {
		client = cl;
		this.maxFrequency=cl.getMaxFrequency();
		this.notifier = notifier;
		count = 0;
		this.newPub = newPub;
		
	}
	public void run()
	{
		count = ((++count)% maxFrequency) +1;
		// to be called per second
	//	client.notification(count, newPub);	
	}
	

}
