package org.reteonstorm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * @author Marinos Mavrommatis
 */
public class UniversalFilterEmitOnce extends BaseBasicBolt {
	private static final long serialVersionUID = -6343942346452143072L;

	private final String delim;
	private final String varIndicator;
	private final String[][] filters;


	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("allBindings"));
	}

	//FIXME: should probably be moved in the prepare method
	public UniversalFilterEmitOnce(String[][] filters, String delim, String variableIndicator) {
		this.delim = delim;
		this.varIndicator = variableIndicator;

		for (String[] subjPredObj : filters)
			if (subjPredObj.length != 3)
				throw new IllegalArgumentException(
						"subjPredObj must be a String array of size 3. Given array of length "
								+subjPredObj.length);

		this.filters = filters;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String[] triple = StringUtils.split(line,delim);
		if (triple.length != 3)
			throw new RuntimeException("Line is not a triple: "+input);

		//separate map for each filter because filters might be coming from different queries => variable name clashes
		List<Map<String,String>> allBindings = new ArrayList<Map<String,String>>(filters.length);
		boolean empty=true;
		filter: for (int i=0; i<filters.length; i++){
			allBindings.add(new HashMap<String,String>()); //to make sure the list expands even if "bindings" is not added
			Map<String, String> bindings = new HashMap<String, String>(3);
			for (int j=0; j<3; j++)
				if (filters[i][j].startsWith(varIndicator)){
					if (bindings.containsKey(filters[i][j])){
						if (!bindings.get(filters[i][j]).equals(triple[j])){
							continue filter;
						}
					}else{
						bindings.put(filters[i][j], triple[j]);
					}
				}else{
					if (!triple[j].equals(filters[i][j])){
						continue filter;
					}
				}
			allBindings.set(i,bindings);
			empty=false;
		}
		if (!empty)
			collector.emit(new Values(allBindings));
	}

	public void cleanup() {}

}
