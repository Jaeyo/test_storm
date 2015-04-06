package org.jaeyo.test_storm.aggregate;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class StringCounter extends BaseAggregator<Map<String, Integer>>{
	private int partitionId;
	private int numPartitions;
	
	public Map<String, Integer> init(Object batchId, TridentCollector collector) {
		return new HashMap<String, Integer>();
	}

	public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
		String loc = tuple.getString(0);
		Integer previousValue =val.get(loc);
		previousValue = previousValue == null ? 0 : previousValue;
		val.put(loc, previousValue+1);
	}

	public void complete(Map<String, Integer> val, TridentCollector collector) {
		collector.emit(new Values(val));
	}
} //class