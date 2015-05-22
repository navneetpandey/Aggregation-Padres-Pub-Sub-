package ca.utoronto.msrg.padres.demo.selectivityComputer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.broker.router.Router;
import ca.utoronto.msrg.padres.broker.router.matching.MatcherException;
import ca.utoronto.msrg.padres.broker.router.matching.rete.ReteMatcher;
import ca.utoronto.msrg.padres.common.message.Advertisement;
import ca.utoronto.msrg.padres.common.message.AdvertisementMessage;
import ca.utoronto.msrg.padres.common.message.Predicate;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.common.util.CommandLine;

public class ComputeSelectivity {

	String subsFile, pubsFile, key;

	BufferedReader subReader, pubReader;
	public static final String CMD_SUBS = "subs";
	public static final String CMD_PUBS = "pubs";
	public static final String CMD_KEY = "key";

	public ComputeSelectivity(String[] args) {
		processArguments(args);
		try {

			subReader = new BufferedReader(new FileReader(subsFile));

		} catch (FileNotFoundException e) {

			e.printStackTrace();
		}

	}

	public double selectivityForSub(String sub) throws MatcherException,
			ParseException, BrokerCoreException, IOException {
		BrokerCore brokerCore = null;
		Router aggregatorRouter;
		String broker1url = "-uri rmi://localhost:1101/Broker1";

		brokerCore = new BrokerCore(broker1url);

		aggregatorRouter = new AggregatorRouter(brokerCore);
		aggregatorRouter.initialize();
		ReteMatcher matcher = new ReteMatcher(brokerCore, aggregatorRouter);

		pubReader = new BufferedReader(new FileReader(pubsFile));

		Advertisement adv = getAdvertisement();
		matcher.add(new AdvertisementMessage(adv, "ou"));

		double matched = 0, total = 0;

		System.out.println(sub);
		matcher.add(new SubscriptionMessage(MessageFactory
				.createSubscriptionFromString(sub), "2"));

		String s;

		while ((s = pubReader.readLine()) != null) {
			// System.out.println(s);
			Publication p = MessageFactory.createPublicationFromString(s);
			if (!matcher.getMatchingSubs(new PublicationMessage(p)).isEmpty())
				matched++;
			total++;
		}

		pubReader.close();

		return matched / total;
	}

	private Advertisement getAdvertisement() {
		Advertisement adv = null;
		try {
			adv = Advertisement.toAdvertisement(pubReader.readLine());
		} catch (IOException e) {
			e.printStackTrace();
		}
		((Predicate) adv.getPredicateMap().get(key)).setOp("=");
		return adv;
	}

	public void printSelectivity() {
		String s;
		try {
			while ((s = subReader.readLine()) != null) {

				System.out.println(subsFile + "\n" + selectivityForSub(s)
						+ "\n\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (MatcherException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (BrokerCoreException e) {
			e.printStackTrace();
		}
	}

	public void processArguments(String[] args) {
		List<String> cliKeys = new ArrayList<String>();
		cliKeys.add(CMD_SUBS + ":");
		cliKeys.add(CMD_PUBS + ":");
		cliKeys.add(CMD_KEY + ":");
		CommandLine cmdLine = new CommandLine(cliKeys.toArray(new String[0]));
		try {
			cmdLine.processCommandLine(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		subsFile = cmdLine.getOptionValue(CMD_SUBS);
		if(subsFile == null)
			usage();
		pubsFile = cmdLine.getOptionValue(CMD_PUBS);
		if(pubsFile == null)
			usage(); 
		key = cmdLine.getOptionValue(CMD_KEY);
		if(key == null)
			usage(); 
	}

	private void usage() {
		 System.out.println("Usage <PADRES_REPO_PATH>/demo/bin/util/computeSelectivity.sh"
		 		+ "\n	-subs <SUB_FILE>"
		 		+ "\n -pubs <PUB_FILE>"
		 		+ "\n -key <Key Field for matching >");
		 System.exit(0);
		
	}

	public static void main(String[] args) {
		ComputeSelectivity cs = new ComputeSelectivity(args);
		cs.printSelectivity();

	}

}
