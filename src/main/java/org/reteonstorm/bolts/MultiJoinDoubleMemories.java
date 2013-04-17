/**
 * TODO
 */
package org.reteonstorm.bolts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.reteonstorm.TopologyMain;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Marinos Mavrommatis
 */
public class MultiJoinDoubleMemories extends BaseBasicBolt {

	private static final String INTERMEDIATE_JOIN_PREFIX = "intermediateJoin";

	private static final int MAX_ENTRIES_EACH = 10000;

	private final List<String> fieldsGroupingVars;

	private Map<String, Memory> memories;

	public MultiJoinDoubleMemories(Map<GlobalStreamId, Set<String>> streamsAndVariables, List<String> fieldsGroupingVars) {
		//variables are variables expected at each incoming stream, not the union of variables
		this.memories = new HashMap<String, Memory>();
		this.fieldsGroupingVars = fieldsGroupingVars;

		//create subnetwork
		Iterator< Entry<GlobalStreamId, Set<String>> > iter = streamsAndVariables.entrySet().iterator();
		Entry<GlobalStreamId, Set<String>> entry;
		entry = iter.next();
		Memory first = new Memory(entry.getKey().toString(), entry.getValue());
		entry = iter.next();
		Memory second = new Memory(entry.getKey().toString(), entry.getValue());
		Memory intermediate = new Memory(INTERMEDIATE_JOIN_PREFIX+0, first, second);
		memories.put(first.name, first);
		memories.put(second.name, second);
		memories.put(intermediate.name, intermediate);
		int i=2;
		while(iter.hasNext()){
			entry = iter.next();
			Memory current = new Memory(entry.getKey().toString(), entry.getValue());
			intermediate = new Memory(INTERMEDIATE_JOIN_PREFIX+i, intermediate, current);
			memories.put(current.name, current);
			memories.put(intermediate.name, intermediate);
			i++;
		}
	}

	public static Set<String> union(Set<String> left, Set<String> right) {
		Set<String> union = new TreeSet<String>();
		union.addAll(left);
		union.addAll(right);
		return union;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String streamName = input.getSourceGlobalStreamid().toString();
		Map<String,String> receivedBindings = (Map<String, String>) input.getValue(0);
		executeHelper(memories.get(streamName), receivedBindings, collector);
	}

	private void executeHelper(Memory memoryToPut, Map<String,String> receivedBindings, BasicOutputCollector collector){ //recursive

		//if this is the last memory, then it's a dummy memory, just emit
		if (memoryToPut.destination == null){
			collector.emit("default", new Values(receivedBindings, SimpleJoinDoubleMemory.extractFieldsGroupingString(receivedBindings, fieldsGroupingVars)));
			return;
		}

		Memory memoryToCompare = memoryToPut.destination.getOtherSource(memoryToPut);

		if (memoryToPut.size() == MAX_ENTRIES_EACH){
			memoryToPut.remove(MAX_ENTRIES_EACH-1);
		}
		memoryToPut.add(receivedBindings);

		Set<String> commonVars = intersection(memoryToCompare.variables, memoryToPut.variables);
		//if no common variables then the Cartesian product will be constructed
		bindings : for (Map<String,String> currentBindings : memoryToCompare){
			for(String varName : commonVars){
				if (!currentBindings.get(varName).equals(receivedBindings.get(varName))){
					continue bindings;
				}
			}
			Map<String,String> combinedBindings = new HashMap<String, String>(); //TODO may be beneficial to initialise with size of union of variables
			combinedBindings.putAll(currentBindings);
			combinedBindings.putAll(receivedBindings);

			executeHelper(memoryToPut.destination, combinedBindings, collector);
		}
	}
	
	public static Set<String> intersection(Set<String> left, Set<String> right) {
		Set<String> clone = new TreeSet<String>(left);
		clone.retainAll(right);
		return clone;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("default", new Fields("bindings", TopologyMain.FIELDS_GROUPING_VAR));
	}

	private class Memory extends ArrayList<Map<String,String>>{

		final String name;
		final Set<String> variables;
		final Memory leftSource;
		final Memory rightSource;
		Memory destination;

		Memory(String name, Set<String> variables) { //receives input from different Bolt
			super();
			this.name = name;
			this.variables = variables;
			this.leftSource = null;
			this.rightSource = null;
		}

		Memory(String name, Memory leftSource, Memory rightSource) {
			super();
			this.name = name;
			this.leftSource = leftSource;
			this.rightSource = rightSource;
			leftSource.destination = this;
			rightSource.destination = this;
			this.variables = union(leftSource.variables, rightSource.variables);
		}

		//		Memory getOtherMemory(){
		//			return this == destination.leftSource ? destination.rightSource : 
		//				this == destination.rightSource ? destination.leftSource : 
		//					null;
		//		}

		Memory getOtherSource(Memory givenSource){
			return givenSource == leftSource ? rightSource : 
				givenSource == rightSource ? leftSource : 
					null;
		}

	}
}

