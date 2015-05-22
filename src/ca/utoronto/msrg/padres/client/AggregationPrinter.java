package ca.utoronto.msrg.padres.client;

import java.awt.Color;

import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.tools.guiclient.GUIClient;

public class AggregationPrinter implements Runnable{

	GUIClient guiClient;
	Publication printPub;
	public AggregationPrinter(GUIClient guiClient, Publication printPub){
		this.printPub = printPub;
		this.guiClient = guiClient;
	}
	public void run() {

		while(printPub.getPairMap().isEmpty()){
			synchronized(this) {
				try {
					wait();
				} catch(InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			guiClient.printNotification("  Pub: " + printPub, Color.BLUE);
			printPub.getPairMap().clear();
		}

	}
}
