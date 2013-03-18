package org.reteonstorm.bolts;

import java.util.Map;

import org.apache.thrift7.TException;

import org.openimaj.kestrel.SimpleKestrelClient;
import org.reteonstorm.TopologyMain;

import backtype.storm.spout.KestrelThriftClient;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class KestrelTerminal extends BaseBasicBolt {
	private static final long serialVersionUID = -7711580647434264410L;
	private static final String KESTREL_IP = "localhost";
	private static final int KESTREL_MEMCACHED_PORT = 22133;
		private KestrelThriftClient client;
//	private SimpleKestrelClient client;
	private final String outputQ;

	public KestrelTerminal(String outputQ) {
		this.outputQ = outputQ;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
				try {
					client = new KestrelThriftClient(TopologyMain.KESTREL_IP, TopologyMain.KESTREL_THRIFT_PORT);
					client.delete_queue(outputQ);
				} catch (TException e) {
					e.printStackTrace();
				}
//		client = new SimpleKestrelClient(KESTREL_IP, KESTREL_MEMCACHED_PORT);
//		client.delete(outputQ);
		//		client.close();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		@SuppressWarnings("unchecked")
		Map<String,String> tuple = (Map<String,String>)input.getValue(0);
		//		System.out.println("KestrelTerminal(ouputQ="+outputQ+"): execute: "+tuple.toString());

		//		client = new SimpleKestrelClient(KESTREL_IP, KESTREL_MEMCACHED_PORT);
//		client.set(outputQ, tuple.toString());
		//		client.close();
				try {
					client.put(outputQ, tuple.toString(), 0);
				} catch (TException e) {
					e.printStackTrace();
				}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public void cleanup() {	
		client.close();
	}

}
