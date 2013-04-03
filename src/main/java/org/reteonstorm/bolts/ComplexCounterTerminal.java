package org.reteonstorm.bolts;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.reteonstorm.TopologyMain;

import scala.actors.threadpool.Arrays;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * TODO:
 * @author Marinos Mavrommatis
 */
public class ComplexCounterTerminal extends CounterTerminal {
	private static final long serialVersionUID = 2578276914993118748L;

//	private final Collection<String> expectedVars;
//	private final String varIndicator;
	/**
	 * @param id
	 * @param var TODO:
	 */
	@SuppressWarnings("unchecked")
	public ComplexCounterTerminal(int id/*, List<String> expectedVars, String varIndicator*/) {
		super(id);
//		this.expectedVars=expectedVars;
//		this.varIndicator=varIndicator;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
//		Map<String,String> tuple = (Map<String,String>)input.getValue(0);
//		for (String part : tuple.keySet()){
//			String[] vars = StringUtils.split(part, varIndicator);
//			if (Arrays.asList(vars).containsAll(this.expectedVars)){
//				System.out.println("Received="+tuple+" CounterTerminal"+id);
				count++;
//			}
//		}
	}

}
