/**
 * TODO
 */
package org.reteonstorm.more;

import java.nio.channels.Pipe.SourceChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Marinos Mavrommatis
 */
public class SimpleJoinSingleMemory extends BaseBasicBolt {

	private static final int MAX_ENTRIES = 100;

	private final Set<String> varsToMatch;
	/* 
	 * Single memory => when full, oldest entry removed irrespective of which queue(s) it belongs to
	 * Also it's a hash => no duplicates as is => time arrived not refreshed when repeated (from any source)
	 */
	private LinkedHashMap<String,Map<String,List<String>>> memory;  

	public SimpleJoinSingleMemory(Set<String> varsToMatch) {
		this.varsToMatch = varsToMatch;
		this.memory = new LinkedHashMap<String, Map<String,List<String>>>(MAX_ENTRIES);
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

//		String source = input.getSourceGlobalStreamid().toString();
//
//		@SuppressWarnings("unchecked")
//		Map<String, String> bindings = (Map<String, String>)input.getValue(0);
//
//		if (!bindings.keySet().containsAll(varsToMatch))
//			throw new RuntimeException("input did not contain all expected bindings");
//
//		List<String> valuesToMatch = new ArrayList<String>(3);
//		for (Entry<String,String> binding : bindings.entrySet()) //order in which entries are returned must be consistent (eg ascending alphabetic)
//			if (varsToMatch.contains(binding.getKey()))
//				valuesToMatch.add(binding.getValue());
//		String valuesJoined = StringUtils.join(valuesToMatch, "");
//
//		if (memory.containsKey(valuesJoined)){
//			Set<String> queuesContainingValues = memory.get(valuesJoined);
//			queuesContainingValues.add(source); //Set.add adds if not added before
//			short numberOfQueues = (short) queuesContainingValues.size();
//			if (numberOfQueues == 2){ //emit if all values if valuesToMatch previously seen from different source
//				collector.emit(new Values(asdf));
//			}else{ //three different queues "created". TODO make it an array instead of list
//				throw new RuntimeException("A triple seems to have come from more than 2 different sources into the same SimpleJoin");
//			}
//		}else{
//			Map<String,List<String>> queuesContainingValues = new TreeMap<String,List<String>>();
//			List<String> valuesNotToMatch = new ArrayList<String>();
//			for (Entry<String,String> binding : bindings.entrySet()) //order in which entries are returned must be consistent (eg ascending alphabetic)
//				if (varsToMatch.contains(binding.getKey()))
//					valuesToMatch.add(binding.getValue());
//			queuesContainingValues.add(source,);
//			memory.put(valuesJoined, queuesContainingValues);
//		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
