/**
 * TODO
 */
package org.reteonstorm.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tools.ant.UnsupportedAttributeException;
import org.reteonstorm.Toolbox;
import org.reteonstorm.TopologyMain;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Marinos Mavrommatis
 */
public class SimpleJoinDoubleMemory extends BaseBasicBolt {
	private static final long serialVersionUID = 1181068441965628220L;

	private static final int MAX_ENTRIES_EACH = 10000;

	private final Set<String> varsToMatch;
	private final String leftStream;
	private final String rightStream;
	private final List<String> fieldsGroupingVars; //intersection between the vars of this join and the next (to be joined with)

	private List<Map<String,String>> leftMemory;
	private List<Map<String,String>> rightMemory;


	public SimpleJoinDoubleMemory(String leftStreamGlobalName, String rightStreamGlobalName, 
			Set<String> varsToMatch, List<String> fieldsGroupingVars) {

		this.leftStream = leftStreamGlobalName;
		this.rightStream = rightStreamGlobalName;
		this.fieldsGroupingVars = fieldsGroupingVars;
		this.varsToMatch = varsToMatch;
		this.leftMemory = new ArrayList<Map<String,String>>(MAX_ENTRIES_EACH);
		this.rightMemory = new ArrayList<Map<String,String>>(MAX_ENTRIES_EACH);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String source = input.getSourceGlobalStreamid().toString();
		List<Map<String, String>> memoryToPut;
		List<Map<String, String>> memoryToCompare;
		if (source.equals(leftStream)){
			memoryToPut = leftMemory;
			memoryToCompare = rightMemory;
		}else if (rightStream.equals(source)){
			memoryToPut = rightMemory;
			memoryToCompare = leftMemory;
		}else
			throw new UnsupportedAttributeException("Source stream not expected by this Join", source);

		if (memoryToPut.size() == MAX_ENTRIES_EACH){
			memoryToPut.remove(MAX_ENTRIES_EACH-1);
		}
		Map<String,String> receivedBindings = (Map<String, String>) input.getValue(0);
		memoryToPut.add(receivedBindings);

		memory : 
			for (Map<String,String> currentBindings : memoryToCompare){
				for(String varName : varsToMatch){
					if (!currentBindings.get(varName).equals(receivedBindings.get(varName))){
						continue memory;
					}
				}
				Map<String,String> combinedBindings = Toolbox.mapUnion(currentBindings, receivedBindings);
				collector.emit(TopologyMain.DEFAULT_STREAM_NAME, new Values(combinedBindings, Toolbox.extractFieldsGroupingString(combinedBindings, fieldsGroupingVars)));
			}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TopologyMain.DEFAULT_STREAM_NAME, new Fields("bindings", TopologyMain.FIELDS_GROUPING_VAR));
	}

}
