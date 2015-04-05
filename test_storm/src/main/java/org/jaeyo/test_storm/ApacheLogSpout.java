package org.jaeyo.test_storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

public class ApacheLogSpout implements IBatchSpout{
	private List<String> logs = new ArrayList<String>();
	private int batchSize;
	private long sleepTime;

	public ApacheLogSpout(int batchSize, long sleepTime) {
		this.batchSize = batchSize;
		this.sleepTime = sleepTime;

		synchronized (ApacheLogSpout.class) {
			try {
				BufferedReader input = new BufferedReader(new InputStreamReader(ApacheLogSpout.class.getResourceAsStream("/access_log")));
				String line = null;
				while((line = input.readLine())!=null)
					logs.add(line);
			} catch (IOException e) {
				e.printStackTrace();
			} //catch
		} //sync
	} //INIT

	public void open(Map conf, TopologyContext context) { }

	public void emitBatch(long batchId, TridentCollector collector) {
		Random rand = new Random();
		for (int i = 0; i < batchSize; i++)
			collector.emit(new Values(logs.get(rand.nextInt(logs.size()))));
		Utils.sleep(sleepTime);
	} //emitBatch

	public void ack(long batchId) { }
	public void close() { }

	public Map getComponentConfiguration() {
		return new Config();
	}

	public Fields getOutputFields() {
		return new Fields("logString");
	}
} //class