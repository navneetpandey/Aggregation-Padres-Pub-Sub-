package ca.utoronto.msrg.padres.tools.guiclient;

import org.apache.log4j.Logger;

import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;

public class DebugBroker implements Runnable{

	String []args;
	 public DebugBroker(String s) {
		 args = s.split(" ");
	 }
	public void run() {
		try {
			BrokerCore brokerCore = new BrokerCore(args);
			brokerCore.initialize();
			//			brokerCore.shutdown();
		} catch (Exception e) {
			// log the error the system error log file and exit
			Logger sysErrLogger = Logger.getLogger("SystemError");
			if (sysErrLogger != null)
				sysErrLogger.fatal(e.getMessage() + ": " + e);
			e.printStackTrace();
			System.exit(1);
		}
		 		
	}

	

}
