package org.reteonstorm.more;

import java.security.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MixedJoin extends BaseBasicBolt {

	Integer id;
	String name;
//	Map<String, Integer> counters;
	Map<String,Set<Integer>> words;

	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup() {
//		System.out.println("-- Word Counter ["+name+"-"+id+"] --");
//		for(Map.Entry<String, Integer> entry : counters.entrySet()){
//			System.out.println(entry.getKey()+": "+entry.getValue());
//		}
	}

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
//		this.counters = new HashMap<String, Integer>();
		this.words = new HashMap<String,Set<Integer>>();
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
		int obj = input.getInteger(1);
//		/**
//		 * If the word dosn't exist in the map we will create
//		 * this, if not We will add 1 
//		 */
//		if(!counters.containsKey(str)){
//			counters.put(str, 1);
//		}else{
//			Integer c = counters.get(str) + 1;
//			counters.put(str, c);
//		}
		if (words.containsKey(subj)){
			Set<Integer> found = words.get(subj);
			found.add(obj);
			if (found.size() == 10){
				collector.emit(new Values(subj));
//				words.remove(str);
			}
		}else{
			Set<Integer> found = new HashSet<Integer>();
			found.add(obj);
			words.put(subj, found);
		}
		
		//need to ack the output colector??
	}
}
