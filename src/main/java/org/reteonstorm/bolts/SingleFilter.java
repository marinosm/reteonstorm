package org.reteonstorm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

/**
 * @author Marinos Mavrommatis
 */
public class SingleFilter extends BaseBasicBolt {
	private static final long serialVersionUID = -6343942346452143072L;

	private final String delim;
	private final String varIndicator;
	private final String[] filter;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("bindings"));
	}

	//FIXME: should probably be moved in the prepare method
	public SingleFilter(String[] filter, String delim, String variableIndicator) {
		if (filter.length != 3)
			throw new IllegalArgumentException(
					"subjPredObj must be a String array of size 3. Given array of length "
							+filter.length);
		this.delim = delim;
		this.varIndicator = variableIndicator;
		this.filter = filter;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String[] triple = StringUtils.split(line,delim);
		if (triple.length != 3)
			throw new RuntimeException("Line is not a triple: "+input);

		//make sure that parts of the filter that are not variables match the respective parts in the input triple
		//also make sure repeated variables receive the same value (ie ?a_foo_?a shouldn't accept A_foo_B)
		Map<String, String> bindings = new TreeMap<String, String>();
		for (int i=0; i<3; i++)
			if (filter[i].startsWith(varIndicator)){
				if (bindings.containsKey(filter[i])){
					if (!bindings.get(filter[i]).equals(triple[i])){
						return;
					}
				}else{
					bindings.put(filter[i], triple[i]);
				}
			}else{
				if (!triple[i].equals(filter[i])){
					return;
				}
			}

		collector.emit(new Values(bindings));
	}

	public void cleanup() {}

}
