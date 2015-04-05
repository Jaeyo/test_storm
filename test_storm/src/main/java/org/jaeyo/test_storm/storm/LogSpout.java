package org.jaeyo.test_storm.storm;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

public class LogSpout implements ITridentSpout<Long>{

	public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
		return new LogBatchCoordinator();
	}

	public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
		return new LogEmitter();
	}

	public Map getComponentConfiguration() {
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("logString");
	} //getOutputFields
} //class