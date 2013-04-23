package org.reteonstorm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.reteonstorm.bolts.SilentCounterTerminal;
import org.reteonstorm.bolts.MultiJoinDoubleMemories;
import org.reteonstorm.bolts.SimpleJoinDoubleMemory;
import org.reteonstorm.bolts.SingleFilter;
import org.reteonstorm.bolts.UniversalFilterMultipleStreams;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class BoltAdder{

	public static GlobalStreamId[] addFilters(TopologyBuilder builder, String[][] filterArrays, boolean nodeSharing){
		List<String[]> filtersToAdd = new ArrayList<String[]>();
		GlobalStreamId[] streams = new GlobalStreamId[filterArrays.length];

		if (nodeSharing)

			Algorithms.shareSimilarFilters(filterArrays, filtersToAdd, streams);

		else{

			filtersToAdd=Arrays.asList(filterArrays);
			for (int i=0; i<streams.length; i++)
				streams[i]=new GlobalStreamId(TopologyMain.FILTER_PREFIX+i, TopologyMain.DEFAULT_STREAM_NAME);

		}

		for (int i=0; i<filtersToAdd.size(); i++){
			String[] filter = filtersToAdd.get(i);

			IBasicBolt filterBolt = new SingleFilter(filter, TopologyMain.DELIM, TopologyMain.VAR_INDICATOR);

			builder.setBolt(TopologyMain.FILTER_PREFIX+i, filterBolt, TopologyMain.FILTER_PARALLELISM)
			.shuffleGrouping(TopologyMain.SPOUT_NAME);
		}

		return streams;
	}

	public static GlobalStreamId[] addSingleFilter(TopologyBuilder builder, String[][] filterArrays, Set<Set<Integer>> groupsOfFilters){
		GlobalStreamId[] streams = new GlobalStreamId[filterArrays.length];

		Map<String,List<String>> fieldsGroupingVarsPerStream = new HashMap<String, List<String>>(streams.length);
		for (Set<Integer> group : groupsOfFilters){
			Set<String[]> filterGroup = Toolbox.extractFilterGroup(filterArrays,group);
			for(int filter : group){
				streams[filter] = new GlobalStreamId(TopologyMain.FILTER_PREFIX, TopologyMain.FILTER_STREAM_PREFIX+filter);
				fieldsGroupingVarsPerStream.put(streams[filter].toString(), new ArrayList<String>(Toolbox.intersection(filterGroup)));
			}
			//TODO: do shuffle grouping when group-size is 1 ?
		}
		IBasicBolt filterBolt = new UniversalFilterMultipleStreams(filterArrays, fieldsGroupingVarsPerStream);
		builder.setBolt(TopologyMain.FILTER_PREFIX, filterBolt, TopologyMain.FILTER_PARALLELISM)
		.shuffleGrouping(TopologyMain.SPOUT_NAME);

		return streams;
	}

	public static GlobalStreamId addJoins(TopologyBuilder builder, String[][] filterArrays, GlobalStreamId[] streams, boolean shareJoins, Set<Set<Integer>> groupsOfFilters){

		GlobalStreamId last = streams[0]; //useful when only 1 entry exists in FILTER_ARRAYS

		if (shareJoins && groupsOfFilters.size()>1){
			Iterator<Set<Integer>> iter = groupsOfFilters.iterator();

			Set<Integer> firstGroup = iter.next();
			Map<GlobalStreamId,Set<String>> firstStreamsAndVariables = new TreeMap<GlobalStreamId, Set<String>>();
			for (int filter : firstGroup)
				firstStreamsAndVariables.put(streams[filter], Toolbox.extractVars(filterArrays[filter]));

			Set<Integer> secondGroup = iter.next();
			Map<GlobalStreamId,Set<String>> secondStreamsAndVariables = new TreeMap<GlobalStreamId, Set<String>>();
			for (int filter : secondGroup)
				secondStreamsAndVariables.put(streams[filter], Toolbox.extractVars(filterArrays[filter]));

			Set<String> firstVarSet = Toolbox.unionAll(firstStreamsAndVariables.values());
			Set<String> secondVarSet = Toolbox.unionAll(secondStreamsAndVariables.values());
			Set<String> fieldsGroupingVars = Toolbox.setIntersection(firstVarSet, secondVarSet);

			GlobalStreamId penultimate = firstStreamsAndVariables.keySet().iterator().next();
			if (firstStreamsAndVariables.size() > 1){
				IBasicBolt firstMultiJoin = new MultiJoinDoubleMemories(firstStreamsAndVariables, new ArrayList<String>(fieldsGroupingVars));
				penultimate = new GlobalStreamId(TopologyMain.MULTI_JOIN_PREFIX+0, TopologyMain.DEFAULT_STREAM_NAME);
				BoltDeclarer declarer = builder.setBolt(penultimate.get_componentId(), firstMultiJoin, TopologyMain.JOIN_PARALLELISM);
				for (GlobalStreamId streamName : firstStreamsAndVariables.keySet())
					declarer.fieldsGrouping(streamName.get_componentId(), streamName.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR));
			}
			last = secondStreamsAndVariables.keySet().iterator().next();
			if (secondStreamsAndVariables.size() > 1){
				IBasicBolt secondMultiJoin = new MultiJoinDoubleMemories(secondStreamsAndVariables, new ArrayList<String>(fieldsGroupingVars));
				last = new GlobalStreamId(TopologyMain.MULTI_JOIN_PREFIX+1, TopologyMain.DEFAULT_STREAM_NAME);
				BoltDeclarer declarer = builder.setBolt(last.get_componentId(), secondMultiJoin, TopologyMain.JOIN_PARALLELISM);
				for (GlobalStreamId streamName : secondStreamsAndVariables.keySet())
					declarer.fieldsGrouping(streamName.get_componentId(), streamName.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR));
			}

			Set<String> unionSoFar = Toolbox.setUnion(firstVarSet,secondVarSet);

			for (int i=2; i<groupsOfFilters.size(); i++){
				Set<Integer> currentGroup = iter.next();
				Map<GlobalStreamId,Set<String>> currentStreamsAndVariables = new TreeMap<GlobalStreamId, Set<String>>();
				for (int filter : currentGroup)
					currentStreamsAndVariables.put(streams[filter], Toolbox.extractVars(filterArrays[filter]));

				Set<String> currentVarSet;
				Set<String> nextFieldsGroupingVars = Toolbox.setIntersection(
						unionSoFar, 
						(currentVarSet = Toolbox.unionAll(currentStreamsAndVariables.values())));

				//join the previously created SimpleJoin and MultiJoin
				String boltName = TopologyMain.SIMPLE_JOIN_PREFIX+(i-1);
				builder.setBolt(boltName, new SimpleJoinDoubleMemory(
						penultimate.toString(), 
						last.toString(), 
						fieldsGroupingVars, new ArrayList<String>(nextFieldsGroupingVars)), TopologyMain.JOIN_PARALLELISM)
						.fieldsGrouping(penultimate.get_componentId(), penultimate.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR))
						.fieldsGrouping(last.get_componentId(), last.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR));
				penultimate = new GlobalStreamId(boltName, TopologyMain.DEFAULT_STREAM_NAME);

				last = currentStreamsAndVariables.keySet().iterator().next();
				if (currentStreamsAndVariables.size() > 1){
					//create new MultiJoin but don't join it yet
					IBasicBolt currentJoinBolt = new MultiJoinDoubleMemories(currentStreamsAndVariables, new ArrayList<String>(fieldsGroupingVars));
					last = new GlobalStreamId(TopologyMain.MULTI_JOIN_PREFIX+i, TopologyMain.DEFAULT_STREAM_NAME);
					BoltDeclarer currentDeclarer = builder.setBolt(last.get_componentId(), currentJoinBolt, TopologyMain.JOIN_PARALLELISM);
					for (GlobalStreamId streamName : currentStreamsAndVariables.keySet())
						currentDeclarer.fieldsGrouping(streamName.get_componentId(), streamName.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR));
				}

				fieldsGroupingVars = nextFieldsGroupingVars;
				unionSoFar = Toolbox.setUnion(unionSoFar, currentVarSet);
			}
			IBasicBolt joinBolt = new SimpleJoinDoubleMemory(
					penultimate.toString(), 
					last.toString(),
					fieldsGroupingVars, new ArrayList<String>(0));

			String boltName = TopologyMain.SIMPLE_JOIN_PREFIX+(groupsOfFilters.size()-1);
			builder.setBolt(boltName, joinBolt, TopologyMain.JOIN_PARALLELISM)
			.fieldsGrouping(penultimate.get_componentId(), penultimate.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR))
			.fieldsGrouping(last.get_componentId(), last.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR));

			last = new GlobalStreamId(boltName, TopologyMain.DEFAULT_STREAM_NAME);

		}else if (filterArrays.length > 1){

			GlobalStreamId leftInput = streams[0];
			GlobalStreamId rightInput = streams[1];

			List<String> fieldsGroupingVars = filterArrays.length == 2 ? new ArrayList<String>(0) :
				new ArrayList<String>(Toolbox.setIntersection(Toolbox.unionVarsUpTo(filterArrays, 1), Toolbox.extractVars(filterArrays[2])));

			IBasicBolt joinBolt = new SimpleJoinDoubleMemory(leftInput.toString(), rightInput.toString(), 
					Toolbox.setIntersection(Toolbox.extractVars(filterArrays[0]), Toolbox.extractVars(filterArrays[1])), fieldsGroupingVars);

			builder.setBolt(TopologyMain.MULTI_JOIN_PREFIX+1, joinBolt, TopologyMain.JOIN_PARALLELISM) 
			.fieldsGrouping(leftInput.get_componentId(), leftInput.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR))
			.fieldsGrouping(rightInput.get_componentId(), rightInput.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR));

			for (int i=2; i<filterArrays.length; i++){
				leftInput = new GlobalStreamId(TopologyMain.MULTI_JOIN_PREFIX+(i-1), TopologyMain.DEFAULT_STREAM_NAME);
				rightInput = streams[i];

				fieldsGroupingVars = i == filterArrays.length-1 ? new ArrayList<String>(0) :
					new ArrayList<String>(Toolbox.setIntersection(Toolbox.unionVarsUpTo(filterArrays, i), Toolbox.extractVars(filterArrays[i+1])));

				joinBolt = new SimpleJoinDoubleMemory(leftInput.toString(), rightInput.toString(),
						Toolbox.setIntersection(Toolbox.unionVarsUpTo(filterArrays, i-1), Toolbox.extractVars(filterArrays[i])), fieldsGroupingVars);

				builder.setBolt(TopologyMain.MULTI_JOIN_PREFIX+i, joinBolt, TopologyMain.JOIN_PARALLELISM)
				.fieldsGrouping(leftInput.get_componentId(), leftInput.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR))
				.fieldsGrouping(rightInput.get_componentId(), rightInput.get_streamId(), new Fields(TopologyMain.FIELDS_GROUPING_VAR));
			}

			last = new GlobalStreamId(TopologyMain.MULTI_JOIN_PREFIX+(filterArrays.length-1), TopologyMain.DEFAULT_STREAM_NAME);
		}

		return last;
	}

	public static void addCounterTerminals(TopologyBuilder builder, String[][] filterArrays, GlobalStreamId[] streams, long numOfObjects){
		for (int i=0; i<streams.length; i++){

			IBasicBolt terminalBolt = new SilentCounterTerminal(i);

			BoltDeclarer declarer=builder.setBolt(TopologyMain.TERMINAL_PREFIX+i, terminalBolt, TopologyMain.TERMINAL_PARALLELISM);

			declarer.shuffleGrouping(streams[i].get_componentId(), streams[i].get_streamId());

			//Determine expected result size FIXME: will give wrong value for "?a_foo_?a". Also I don't think it handles the case when there is a filter without a variable that actually limits the result.
			final long[] sizes = new long[]{TopologyMain.SUBJECTS.length, TopologyMain.PREDICATES.length, numOfObjects};
			long expectedResultSize = 1;
			for (int j=0; j<TopologyMain.FILTER_LENGTH; j++)
				if (filterArrays[i][j].startsWith(TopologyMain.VAR_INDICATOR))
					expectedResultSize*=sizes[j];
			System.out.println("ExpectedResultSize"+i+'='+expectedResultSize);
		}
	}
	public static void addSingleCounterTerminal(TopologyBuilder builder, String[][] filterArrays, GlobalStreamId stream, long numOfObjects) {
		IBasicBolt terminalBolt = new SilentCounterTerminal(0);
		
		BoltDeclarer declarer=builder.setBolt(TopologyMain.TERMINAL_PREFIX+0, terminalBolt, TopologyMain.TERMINAL_PARALLELISM);
		
		declarer.shuffleGrouping(stream.get_componentId(), stream.get_streamId());
		
		//Determine expected result size FIXME: will give wrong value for "?a_foo_?a". Also I don't think it handles the case when there is a filter without a variable that actually limits the result.
		final long[] sizes = new long[]{TopologyMain.SUBJECTS.length, TopologyMain.PREDICATES.length, numOfObjects};
		long expectedResultSize = 1;
		Set<String> alreadyConsidered = new HashSet<String>();
		expectedResultSize = 0;
		for (int j=0; j<filterArrays.length; j++){
			long temp = 1;
			for (int k=0; k<TopologyMain.FILTER_LENGTH; k++)
				if (filterArrays[j][k].startsWith(TopologyMain.VAR_INDICATOR) && !alreadyConsidered.contains(filterArrays[j][k])){
					temp*=sizes[k];
					alreadyConsidered.add(filterArrays[j][k]);
				}
			expectedResultSize += temp > 1 ? temp : 0;
		}
		System.out.println("ExpectedResultSize0="+expectedResultSize);
	}
}

