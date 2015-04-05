package org.jaeyo.test_storm.function;

import org.jaeyo.test_storm.vo.ApacheLogVO;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class ParseApacheLog extends BaseFunction {

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String logString = tuple.getStringByField("logString");
		ApacheLogVO apacheLogVo = parse(logString);
		collector.emit(new Values(apacheLogVo));
	} // execute

	private static ApacheLogVO parse(String log){
		ApacheLogVO logVO = new ApacheLogVO();
		logVO.setIp(log.split(" ")[0].trim());
		logVO.setDateTime(log.split("\\[")[1].split("\\]")[0].trim());
		logVO.setRequest(log.split("\\\"")[1].trim());
		logVO.setResponse(log.split("\\\"")[2].split(" ")[0].trim());
		logVO.setBytesSent(Long.parseLong(log.split("\\\"")[2].split(" ")[1].trim()));
		return logVO;
	} //parse
	
	public static BaseFunction parseIp() {
		return new BaseFunction() {
			public void execute(TridentTuple tuple, TridentCollector collector) {
				String logString = tuple.getStringByField("logString");
				ApacheLogVO logVO = parse(logString);
				collector.emit(new Values(logVO.getIp()));
			} // execute
		};
	} // parseIp

	public static BaseFunction parseRequest() {
		return new BaseFunction() {
			public void execute(TridentTuple tuple, TridentCollector collector) {
				String logString = tuple.getStringByField("logString");
				ApacheLogVO logVO = parse(logString);
				collector.emit(new Values(logVO.getRequest()));
			} // execute
		};
	} // parseRequest
} // class