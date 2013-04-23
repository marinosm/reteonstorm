package org.reteonstorm.more;

import java.util.Map;

import org.reteonstorm.bolts.CounterTerminal;

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
		@SuppressWarnings("unchecked")
		Map<Integer,Map<String, String>> matched = (Map<Integer,Map<String, String>>)input.getValue(0);
		if (matched.containsKey(id) && matched.get(id).size()>0)
			count++;
	}

}
