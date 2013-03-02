package org.reteonstorm.more;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ObjSubjJoin extends BaseBasicBolt {

	Integer id;
	String name;
//	Map<String, Integer> counters;
	Set<String> varsFound;

	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup(){}

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.varsFound = new HashSet<String>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("letter"));
	}


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String subj = input.getString(0);
		String obj  = input.getString(1);
		String ofInterest = obj == null? subj : obj;  
		if (varsFound.contains(ofInterest)){		// assuming input is unique!! (for now)
			collector.emit(new Values(ofInterest));
			varsFound.remove(ofInterest);
		}else{
			varsFound.add(ofInterest);
		}
	}
}
