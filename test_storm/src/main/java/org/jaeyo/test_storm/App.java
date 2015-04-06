package org.jaeyo.test_storm;

import org.jaeyo.test_storm.aggregate.StringCounter;
import org.jaeyo.test_storm.filter.Print;
import org.jaeyo.test_storm.function.ParseApacheLog;
import org.jaeyo.test_storm.function.ToUpperCase;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.MemoryMapState.Factory;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class App {
	public static void main(String[] args) {
		System.out.println("start");
		
		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("test", config, getSimpleUpperTopology());
//		cluster.submitTopology("test", config, getParallelTopology());
//		cluster.submitTopology("test", config, getProjectTopology());
//		cluster.submitTopology("test", config, getParseVOTopology());
//		cluster.submitTopology("test", config, getParseIpTopology());
//		cluster.submitTopology("test", config, getGroupCountTopology());
		cluster.submitTopology("test", config, getStringCounterTopology());
		
		System.out.println("submited");
		
		Utils.sleep(10000);
		
		System.out.println("end");
	} // main

	private static StormTopology getSimpleUpperTopology() {
		TridentTopology topology = new TridentTopology();
		
		topology.newStream("test1", new ApacheLogSpout(1, 1000))
			.each(new Fields("logString"), new ToUpperCase(), new Fields("upperLogString"))
			.each(new Fields("upperLogString", "logString"), new Print());
		
		return topology.build();
	} // getSimpleUpperTopology
	
	private static StormTopology getParallelTopology() {
		TridentTopology topology = new TridentTopology();
		
		topology.newStream("test1", new ApacheLogSpout(1, 1000))
			.each(new Fields("logString"), new ToUpperCase(), new Fields("upperLogString"))
			.parallelismHint(2)
			.each(new Fields("upperLogString"), new Print());
		
		return topology.build();
	} // getParallelTopology
	
	private static StormTopology getProjectTopology(){
		TridentTopology topology = new TridentTopology();
		
		topology.newStream("test1", new ApacheLogSpout(1, 1000))
			.each(new Fields("logString"), new ToUpperCase(), new Fields("upperLogString"))
			.project(new Fields("upperLogString"))
			.each(new Fields("upperLogString"), new Print());
		
		return topology.build();
	} //getProjectTopology
	
	private static StormTopology getParseVOTopology(){
		TridentTopology topology = new TridentTopology();
		
		topology.newStream("test1", new ApacheLogSpout(1, 1000))
			.each(new Fields("logString"), new ParseApacheLog(), new Fields("logVO"))
			.each(new Fields("logVO"), new Print());
		
		return topology.build();
	} //getParseVOTopology
	
	private static StormTopology getParseIpTopology(){
		TridentTopology topology = new TridentTopology();
		
		topology.newStream("test1", new ApacheLogSpout(1, 500))
			.each(new Fields("logString"), ParseApacheLog.parseIp(), new Fields("ip"))
			.project(new Fields("ip"))
			.each(new Fields("ip"), new Print());
		
		return topology.build();
	} //getParseVOTopology
	
	private static StormTopology getGroupCountTopology(){
		TridentTopology topology = new TridentTopology();
		
		topology.newStream("test1", new ApacheLogSpout(10, 500))
			.each(new Fields("logString"), ParseApacheLog.parseIp(), new Fields("ip"))
			.groupBy(new Fields("ip"))
			.aggregate(new Count(), new Fields("count"))
			.each(new Fields("ip", "count"), new Print());
		
		return topology.build();
	} //getGroupCountTopology
	
	private static StormTopology getGroupCountPersistentTopolgoy(){
		TridentTopology topology = new TridentTopology();
		
		topology.newStream("test1", new ApacheLogSpout(100, 500))
			.each(new Fields("logString"), ParseApacheLog.parseIp(), new Fields("ip"))
			.groupBy(new Fields("ip"))
			.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
			.newValuesStream().each(new Fields("ip", "count"), new Print());
		
		return topology.build();
	} //getGroupCountPersistentTopology
	
	private static StormTopology getStringCounterTopology(){
		TridentTopology topology = new TridentTopology();
		
		topology.newStream("test1", new ApacheLogSpout(100, 500))
			.each(new Fields("logString"), ParseApacheLog.parseIp(), new Fields("ip"))
			.aggregate(new Fields("ip"), new StringCounter(), new Fields("aggregated_result"))
			.each(new Fields("aggregated_result"), new Print());
		
		return topology.build();
	} //getGroupCountPersistentTopology
} // class