package ca.utoronto.msrg.padres.demo.stockquote;

import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Advertisement;
import ca.utoronto.msrg.padres.common.message.Predicate;
import ca.utoronto.msrg.padres.demo.dataset.DatasetPublisherClient;



/**
 * 
 * @author cheung
 * 
 *         Publishes stock quote publications just like the publisher client in
 *         the simulator
 * 
 *         Features: - delivery delay logging - does not shutdown if publisher
 *         is not connected with anyone
 * 
 */
public class StockPublisher extends DatasetPublisherClient {
 
	public static final String PUB_DELIMIT_CHAR = "/";
	public StockPublisher(ClientConfig clientConfig, String pubSymbol,
			double pubRate, int replication, String finishDir) throws ClientException {
		super(clientConfig, pubSymbol, pubRate, replication, finishDir);

	}

	@Override
	protected void customizeAdvertisement(Advertisement adv) {
		((Predicate) adv.getPredicateMap().get("symbol")).setOp("eq");
	}

	@Override
	protected String getDataPath() {

		if(clientConfig.clientProps.getProperty("Stock.DataPath")!=null)
			return clientConfig.clientProps.getProperty("Stock.DataPath");
		else 
		return ClientConfig.PADRES_HOME + "/demo/data/stockquotes/";
	}

	@Override
	protected String getDataFilePath(String symbol) {
		if(clientConfig.clientProps.getProperty("Stock.DataPath")!=null) {
			System.out.println("Path is:" + getDataPath() + symbol+".csv_out" );
			return getDataPath() + symbol+".csv_out";
		}
		else {
			System.out.println("Path is:" + getDataPath() + symbol );
			return getDataPath() + symbol;
		}
	}

	@Override
	protected String getDefaultAdvertisement() {

		return "[class,eq,'STOCK'],[symbol,isPresent,'A'],[open,isPresent,1],"
				+ "[high,isPresent,1],[low,isPresent,1],[close,isPresent,1],"
				+ "[volume,isPresent,1],[date,isPresent,'A']";
	}

}
