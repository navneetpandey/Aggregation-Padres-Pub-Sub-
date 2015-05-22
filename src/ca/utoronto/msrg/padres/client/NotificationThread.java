package ca.utoronto.msrg.padres.client;

import java.io.Serializable;
import java.util.Iterator;

import ca.utoronto.msrg.padres.common.message.Publication;

class NotificationThread implements Runnable{
	Publication printPub;
	Publication agrPub;
	AggregationPrinter aggregationPrinter;
	NotificationTask notificationTask;
	NotificationThread(Publication printPub, Publication agrPub, AggregationPrinter aggregationPrinter, NotificationTask notificationTask){
		this.printPub = printPub;
		this.agrPub = agrPub;
		this.aggregationPrinter = aggregationPrinter;
		this.notificationTask = notificationTask;
	}
	@Override
	public void run() {
		while(true) {
			if(printPub.getPairMap().isEmpty() && agrPub.getPairMap().isEmpty()) {

				//wait for aggregation message is to be ready
				synchronized(notificationTask) {
					try {
						notificationTask.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					Iterator<String> iter = agrPub.getPairMap().keySet().iterator();
					String attribute;
					while(iter.hasNext()) {
						attribute = iter.next();
						printPub.addPair(attribute, agrPub.getPairMap().get(attribute));

					}
					agrPub.getPairMap().clear();
				}

				//Receieved agr message notify it for print 
				synchronized(aggregationPrinter) {
					aggregationPrinter.notify();
				}
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 

	}

}