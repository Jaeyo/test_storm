package org.jaeyo.test_storm;

import java.util.HashMap;
import java.util.Map;

import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class StringCounter extends BaseAggregator<Map<String, Integer>>{

	public Map<String, Integer> init(Object batchId, TridentCollector collector) {
		return new HashMap<String, Integer>();
	}

	public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
		String loc = tuple.get
	}

	public void complete(Map<String, Integer> val, TridentCollector collector) {
		// TODO Auto-generated method stub
		
	}

}
