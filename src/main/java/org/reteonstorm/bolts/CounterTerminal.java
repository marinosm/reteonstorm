package org.reteonstorm.bolts;

import java.util.Map;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author Marinos Mavrommatis
 */
public class CounterTerminal extends BaseBasicBolt {
	private static final long serialVersionUID = 6037623011308690311L;

	private int id;
	private long count;

	public CounterTerminal(int id) {
		this.id = id;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		count = 0;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("Received="+(Map<String,String>)input.getValue(0)+"CounterTerminal"+id); //TODO logger.debug
		count++;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public void cleanup() {
		System.out.println("CounterTerminal"+id+": partialCount="+count);
	}
}
