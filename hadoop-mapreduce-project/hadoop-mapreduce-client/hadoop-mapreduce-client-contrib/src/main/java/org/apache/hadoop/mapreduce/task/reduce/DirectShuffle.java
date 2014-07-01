package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;

import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.ShuffleConsumerPlugin;

public class DirectShuffle<K,V> implements ShuffleConsumerPlugin<K, V>,
		ExceptionReporter {

	@Override
	public void reportException(Throwable t) {
		// TODO Auto-generated method stub

	}

	@Override
	public void init(ShuffleConsumerPlugin.Context<K, V> context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public RawKeyValueIterator run() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
}
