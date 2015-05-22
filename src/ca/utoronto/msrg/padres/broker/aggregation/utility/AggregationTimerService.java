package ca.utoronto.msrg.padres.broker.aggregation.utility;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import ca.utoronto.msrg.padres.broker.aggregation.FineGrainAdaptiveAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.OptimalAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.AbstractAggregator;
import ca.utoronto.msrg.padres.broker.aggregation.collector.AbstractCollector;
import ca.utoronto.msrg.padres.broker.aggregation.collector.DummyCollector;

public class AggregationTimerService extends Timer {

	private static final long DELAY = 1;

	final int MILLISECOND = 1000;

	private int WAITING_TIME = 100;

	private int PRINT_FREQUENCY_MILLI = 10000;

	// Map<DummyCollector, String> registeredCollectors;

	public AggregationTimerService(int experimentDuration) {

		schedule(new TimerTask() {
			public void run() {
				shutDown();
			}
		}, getFutureDate(experimentDuration * MILLISECOND));

		printTimer();
		// registeredCollectors = new HashMap<DummyCollector, String>();

	}

	public void registerAggregator(final AbstractAggregator aggregator) {
		scheduleForNextNotification(aggregator);
	}

	public void scheduleForNextNotification(final AbstractAggregator aggregator) {
		schedule(
				new TimerTask() {
					public void run() {
						/*
						 * System.out.println("Publish Result called at "+
						 * System.currentTimeMillis()+"\n\n");
						 */
						aggregator.publishResult(true);
						scheduleForNextNotification(aggregator);
					}
				},
				getFutureDate((int) ((aggregator
						.getStartTimeOfTheCurrentWindow() + aggregator
						.getAggregationID().getWindowSize() * MILLISECOND) - System
						.currentTimeMillis()))); // Thread.sleep(1001 -
													// (System.currentTimeMillis()
													// % (1000)));
	}

	public void shutDown() {
		this.cancel();
		this.purge();

	}

	public Date getFutureDate(int offset) {
		Calendar calendar = Calendar.getInstance(); // gets a calendar using the
													// default time zone and
													// locale.
		calendar.add(Calendar.MILLISECOND, offset);
		return calendar.getTime();
	}

	public void addPubInCollector(final AbstractCollector collector,
			long expireTime) {
		// String tempWindowID = registeredCollectors.get(dummyCollector);
		// if (tempWindowID == null || tempWindowID != windowID) {
		// Date futureDate = getFutureDate(WAITING_TIME);
		if (collector instanceof DummyCollector)
			System.out.println("[AGG_TIMER_SERVICE]addPubInDummyCollector=>"
					+ "at" + System.currentTimeMillis() + "scheduled at"
					+ expireTime);
		else
			System.out.println("[AGG_TIMER_SERVICE]addPubInNormalCollector=>"
					+ "at" + System.currentTimeMillis() + "scheduled at"
					+ expireTime);
		if (expireTime > 0)
			addDummyTask(collector, new Date(expireTime + DELAY));
		else
			collector.sendExpiredMessage();
	}

	public void addDummyTask(final AbstractCollector collector, Date futureDate) {
		schedule(new TimerTask() {
			public void run() {
				// dummyCollector.sendExpiredMessage();
				collector.sendExpiredMessage();
			}
		}, futureDate);
	}

	// }

	public void addOptimalTask(final OptimalAggregationEngine aggregationEngine) {
		schedule(
				new TimerTask() {
					public void run() {
						aggregationEngine.Notify();
						addOptimalTask(aggregationEngine);
					}

				},
				getFutureDate((int) (aggregationEngine.COLLECTING_TIMEOUT - System
						.currentTimeMillis()
						% aggregationEngine.COLLECTING_TIMEOUT)));
	}

	public void addFGTask(
			final FineGrainAdaptiveAggregationEngine aggregationEngine) {
		schedule(
				new TimerTask() {
					public void run() {
						aggregationEngine.notifyFromAggregatorTimer(0);
						addFGTask(aggregationEngine);
					}

				},
				getFutureDate((int) (aggregationEngine.WAITINGTIME - System
						.currentTimeMillis() % aggregationEngine.WAITINGTIME)));
	}

	public void printTimer() {
		schedule(new TimerTask() {
			public void run() {
				try {
					Stats.getInstance().printResult();
				} catch (IOException e) {
					e.printStackTrace();
				}
				printTimer();
			}
		}, getFutureDate(PRINT_FREQUENCY_MILLI));
	}

}
