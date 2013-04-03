package org.reteonstorm.bolts;

import java.util.List;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * TODO:
 * @author Marinos Mavrommatis
 */
public class CounterTerminalReceivePack extends CounterTerminal {
	private static final long serialVersionUID = 2578276914993118748L;

	public CounterTerminalReceivePack(int id) {
		super(id);
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		List<Map<String, String>> matched = (List<Map<String, String>>)input.getValue(0);
		if (matched.get(id).size() > 0)
			count++;
	}

}
