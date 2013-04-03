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

import org.apache.commons.lang.StringUtils;

/**
 * @author Marinos Mavrommatis
 */
public class SuperBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -6343942346452143072L;

	private final String delim;
	private final String varIndicator;
	private final String[][] filters;


	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("bindings"));
	}

	//FIXME: should probably be moved in the prepare method
	public SuperBolt(String[][] filters, String delim, String variableIndicator) {
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

	}

	public void cleanup() {}

}
