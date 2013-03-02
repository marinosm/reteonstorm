package org.reteonstorm.more;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

public class WordNormalizer extends BaseBasicBolt {
	
	private final int obj;

	public WordNormalizer(int obj) {
		super();
		this.obj = obj;
	}

	public void cleanup() {}

	/**
	 * The bolt will receive the line from the
	 * words file and process it to Normalize this line
	 * 
	 * The normalize will be put the words in lower case
	 * and split the line to get all words in this 
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
        String[] words = line.split(" ");
        String subj = words[0];
        int obj = Integer.parseInt(words[1]);
        if (obj == this.obj){ 
        	collector.emit(new Values(subj));
        }
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {}

	/**
	 * The bolt will only emit the field "word" 
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("subj"));
	}
}
