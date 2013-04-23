package org.reteonstorm.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * TODO:
 * @author Marinos Mavrommatis
 */
public class SilentCounterTerminal extends CounterTerminal {
	private static final long serialVersionUID = 2578276914993118748L;

	public SilentCounterTerminal(int id) {
		super(id);
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		count++;
	}

}
