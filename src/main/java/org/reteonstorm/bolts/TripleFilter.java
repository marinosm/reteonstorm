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

	private final String delim;
	private final char varIndicator;

	private final String[] filters;


	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("bindings"));
	}

	//FIXME: should probably be moved in the prepare method
	public TripleFilter(String[] subjPredObj, String delim, char variableIndicator) {
		this.delim = delim;
		this.varIndicator = variableIndicator;

		if (subjPredObj.length != 3)
			throw new IllegalArgumentException(
					"subjPredObj argument must be a String array with three (possibly null) entries. Given String array of length "
							+subjPredObj.length);

		this.filters = subjPredObj;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String[] triple = line.split(delim);
		if (triple.length != 3)
			throw new IllegalArgumentException("Line is not a triple: "+input);

		boolean triplePassesFilters = true;
		for (int i=0; i<3; i++){
			if (filters[i].charAt(0)!=varIndicator && !triple[i].equals(filters[i])){
				triplePassesFilters = false;
				break;
			}
		}

		Map<String, String> bindings = new HashMap<String, String>(3);
		if (triplePassesFilters){
			for (int i=0; i<3; i++)
				if (filters[i].charAt(0)==varIndicator)
					bindings.put(filters[i], triple[i]);
			collector.emit(new Values(bindings));
		}
	}

	public void cleanup() {}

}
