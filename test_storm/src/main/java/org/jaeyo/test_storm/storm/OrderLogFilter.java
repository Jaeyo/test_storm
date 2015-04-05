package org.jaeyo.test_storm.storm;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class OrderLogFilter extends BaseFilter{

	public boolean isKeep(TridentTuple tuple) {
		String log = tuple.getStringByField("logString");
		if(log.startsWith("E_"))
			return true;
		return false;
	} //isKeep
} //class