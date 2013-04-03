package org.reteonstorm.bolts;

import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * TODO:
 * @author Marinos Mavrommatis
 */
public class CounterTerminalReceiveEach extends CounterTerminal {
	private static final long serialVersionUID = 2578276914993118748L;

	public CounterTerminalReceiveEach(int id) {
		super(id);
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		int filter_index = input.getInteger(0);
		Map<String, String> bindings = (Map<String, String>)input.getValue(1);
		if (filter_index==id)
			count++;
	}

}
