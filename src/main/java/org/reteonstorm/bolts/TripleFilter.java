package org.reteonstorm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class TripleFilter extends BaseBasicBolt {
	private static final long serialVersionUID = -6343942346452143072L;
	
	private final int id;
	private final String name;
	private final String[] filters;

	//FIXME: should probably be moved in the prepare method
	public TripleFilter(int id, String name, String[] subjPredObj) {
		this.id = id;
		this.name = name;
		
		if (subjPredObj.length != 3)
			throw new IllegalArgumentException(
					"subjPredObj argument must be a String array with three (possibly null) entries. Given String array of length "
							+subjPredObj.length);
				
		this.filters = subjPredObj;
	}

	public void cleanup() {}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
        String[] triple = line.split(" ");
        if (triple.length != 3)
        	throw new IllegalArgumentException("Line is not a triple: "+input);
        
        boolean triplePassesFilters = true;
        for (int i=0; i<3; i++)
        	if (filters[i].charAt(0)!='?' && !triple[i].equals(filters[i])){
       			triplePassesFilters = false;
        		break;
        	}
        
        Map<String, String> bindings = new HashMap<String, String>(3);
        if (triplePassesFilters){
        	for (int i=0; i<3; i++)
        		if (filters[i].charAt(0)=='?')
        			bindings.put(filters[i], triple[i]);
        	collector.emit(new Values(bindings, bindings.containsValue("subj=a") || bindings.containsValue("subj=b")));
        }
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("subj", "value=a|b"));
	}
}
