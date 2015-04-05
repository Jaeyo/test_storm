package org.jaeyo.test_storm.storm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.utils.Utils;

public class LogEmitter implements Emitter<Long> {

	public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
		System.out.println("###DEBUG, LogEmiiter.emitBatch, tx : " + tx.toString());
		List<String> logs = new ArrayList<String>();
		for (int i = 0; i < 10; i++)
			logs.add("E_" + UUID.randomUUID().toString());
		for (int i = 0; i < 10; i++)
			logs.add("R_" + UUID.randomUUID().toString());

		Utils.sleep(500);

		for (String log : logs)
			collector.emit(Arrays.<Object> asList(log));
	} // emitBatch

	public void success(TransactionAttempt tx) {
		System.out.println("###DEBUG, LogEmitter.sucess, tx : " + tx.toString());
	} // success

	public void close() {
		System.out.println("###DEBUG, LogEmitter.close");
	} // close
}