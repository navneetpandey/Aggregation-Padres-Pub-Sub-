package ca.utoronto.msrg.padres.demo.traffic;

import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Advertisement;
import ca.utoronto.msrg.padres.common.message.Predicate;
import ca.utoronto.msrg.padres.demo.dataset.DatasetPublisherClient;

public class TrafficPublisher extends DatasetPublisherClient implements Runnable {

 
	public TrafficPublisher(ClientConfig clientConfig, String pubSymbol,
			double pubRate, int replication, String finishDir) throws ClientException {
		super(clientConfig, pubSymbol, pubRate, replication, finishDir);

	}

	@Override
	protected void customizeAdvertisement(Advertisement adv) {
		((Predicate) adv.getPredicateMap().get("ucID")).setOp("=");
	}

	@Override
	protected String getDataPath() {
		if(clientConfig.clientProps.getProperty("Traffic.DataPath")!=null)
			return clientConfig.clientProps.getProperty("Traffic.DataPath");
		else
			return ClientConfig.PADRES_HOME + "demo/data/trafficPubs/";
	}

	@Override
	protected String getDataFilePath(String symbol) {
		return getDataPath() + symbol + ".data";
	}

	@Override
	protected String getDefaultAdvertisement() {

		return "[class,eq,'traffic'],[date,isPresent,'A'],[ucID,isPresent,1],"
				+ "[name,isPresent,'A'],[addres,isPresent,'A'],[lat,isPresent,1],[lon,isPresent,1],[lane,isPresent,1],"
				+ "[occu_per,isPresent,1],[speed,isPresent,1],[no_of_vehicle,isPresent,1],[uid,isPresent,1],[region,isPresent,'A']";
	}

}
