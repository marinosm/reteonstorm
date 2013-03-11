package org.reteonstorm.bolts;

import java.util.Map;

import org.apache.thrift7.TException;

//import org.openimaj.kestrel.SimpleKestrelClient;

import backtype.storm.spout.KestrelThriftClient;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class KestrelTerminal extends BaseBasicBolt {
	private static final long serialVersionUID = -7711580647434264410L;
	private static final String OUTPUT_QUEUE = "output_Q";
	private static final String KESTREL_IP = "localhost";
	private static final int KESTREL_PORT = 22133;
//	private SimpleKestrelClient client = null;
	private KestrelThriftClient client;
	
	@Override
	public void cleanup() {	
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {	
//		client = new SimpleKestrelClient(KESTREL_IP, KESTREL_PORT);
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		@SuppressWarnings("unchecked")
		Map<String,String> tuple = (Map<String,String>)input.getValue(0);
		
		try {
			System.out.println(tuple);
			client = new KestrelThriftClient(KESTREL_IP, KESTREL_PORT);
			client.put(OUTPUT_QUEUE, tuple.toString(), 0);
			client.close(); 
		} catch (TException e) {
			e.printStackTrace();
		} //set(OUTPUT_QUEUE, tuple.toString());
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}
