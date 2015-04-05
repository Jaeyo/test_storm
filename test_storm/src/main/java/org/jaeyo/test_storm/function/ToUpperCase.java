package org.jaeyo.test_storm.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class ToUpperCase extends BaseFunction{

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String logString = tuple.getStringByField("logString");
		collector.emit(new Values(logString.toUpperCase()));
	} //execute
} //class