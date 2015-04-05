package org.jaeyo.test_storm.storm;

import storm.trident.spout.ITridentSpout.BatchCoordinator;

public class LogBatchCoordinator implements BatchCoordinator<Long>{

	public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
		System.out.println("###DEBUG, LogBatchCoordinator initialize transaction");
		if(prevMetadata==null)
			return 1L;
		return prevMetadata + 1L;
	}

	public void success(long txid) {
		System.out.println("###DEBUG, LogBatchCoordinator success");
		
	}

	public boolean isReady(long txid) {
		System.out.println("###DEBUG, LogBatchCoordinator isReady, txid : " + txid);
		return true;
	}

	public void close() {
		System.out.println("###DEBUG, LogBatchCoordinator close");
	}
} //class