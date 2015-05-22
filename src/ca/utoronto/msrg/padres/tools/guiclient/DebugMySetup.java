package ca.utoronto.msrg.padres.tools.guiclient;

public class DebugMySetup {

	private DebugGUIClient gc1;
	private DebugGUIClient gc2;
	private DebugGUIClient gc3;
	private DebugBroker db1;
	private DebugBroker db2;
	private DebugBroker db3;
	private DebugBroker db4;
	private DebugBroker db5;

	public DebugMySetup() {
		String s = "-i Client2 -b socket://localhost:1101/Broker1";
		String[] myarg1 = s.split(" ");
		s = "-i Client3 -b socket://localhost:1103/Broker3";
		String[] myarg2 = s.split(" ");
		s = "-i Client4 -b socket://localhost:1104/Broker4";
		String[] myarg3 = s.split(" ");

		gc1 = new DebugGUIClient(myarg1);
		gc2 = new DebugGUIClient(myarg2);
		gc3 = new DebugGUIClient(myarg3);

		s = "-uri socket://localhost:1101/Broker1 -n socket://localhost:1102/Broker2";
		db1 = new DebugBroker(s);


		// core brokers
		s = "-uri socket://localhost:1102/Broker2";
		db2 = new DebugBroker(s);

		s = "-uri socket://localhost:1103/Broker3 -n socket://localhost:1102/Broker2";
		db3 = new DebugBroker(s);

		s = "-uri socket://localhost:1104/Broker4 -n socket://localhost:1103/Broker3";
		db4 = new DebugBroker(s);

		s = "-uri socket://localhost:1105/Broker5 -n socket://localhost:1104/Broker4";
		db5 = new DebugBroker(s);
	}

	public static void main(String[] args) {

		DebugMySetup dms = new DebugMySetup();

		Thread tc1 = new Thread(dms.db2);
		tc1.start();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Thread tc8 = new Thread(dms.db3);
		tc8.start();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Thread tc2 = new Thread(dms.db4);
		tc2.start();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Thread tc3 = new Thread(dms.db1);
		tc3.start();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Thread tc4 = new Thread(dms.db5);
		tc4.start();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Thread tc5 = new Thread(dms.gc1);
		tc5.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Thread tc6 = new Thread(dms.gc2);
		tc6.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Thread tc7 = new Thread(dms.gc3);
		tc7.start();
	}
}
