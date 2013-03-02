package org.reteonstorm.bolts;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DummyJoin extends BaseBasicBolt {
	private static final long serialVersionUID = 5947666903329069173L;
	
	Integer id;
	String name;
//	Map<String,Map<String,List<String>>> joined;
//	Map<String,List<String>> input1;
//	Map<String, Map<String, Set<Integer>>> joined;	//TODO: can this really be global?

	@Override
	public void cleanup() {	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
//		this.joined = new HashMap<String, Map<String,Set<Integer>>>();
//		this.joined = new HashMap<String, Map<String,List<String>>>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("letter"));
	}

	@Override
	@SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
		String sourceName = input.getString(0);
		Map<String,String> bindings = (Map<String, String>) input.getValue(1);
//		if (joined.containsKey(key))
		
		collector.emit(new Values(bindings));
	}
}
