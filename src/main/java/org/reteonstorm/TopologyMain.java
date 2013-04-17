package org.reteonstorm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift7.TException;
import org.reteonstorm.bolts.ComplexCounterTerminal;
import org.reteonstorm.bolts.CounterTerminal;
import org.reteonstorm.bolts.CounterTerminalReceiveEach;
import org.reteonstorm.bolts.CounterTerminalReceivePack;
import org.reteonstorm.bolts.MultiJoinDoubleMemories;
import org.reteonstorm.bolts.PassThroughBolt;
import org.reteonstorm.bolts.SimpleJoinDoubleMemory;
import org.reteonstorm.bolts.SingleFilter;
import org.reteonstorm.bolts.UniversalFilterEmitEach;
import org.reteonstorm.bolts.UniversalFilterEmitOnce;
import org.reteonstorm.bolts.UniversalFilterMultipleStreams;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.KestrelThriftClient;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * This is to test whether there is measurable benefit in avoiding to create multiple identical Storm Bolts
 * (for now the identical filter Bolts consume identical input)
 * 
 * TODO: size of messages vs number of messages
 * TODO: how about sharing all filters? (Different gains on cluster?) 
 * TODO: tuning parallelism on real cluster
 * TODO: how about Joins? (each has different tuple source, their computation is more expensive, memory consumption affects completeness of result => don't just measure throughput)
 * TODO: filters could have different input source if filter after filter
 * TODO: experiment with Tasks as well. (Each thread calling one exec method and then another)
 * TODO: Fields grouping necessary for certain operations. In that case maybe higher parallelism is necessary
 * TODO: use various groupings to subscribe to the shared/duplicated node
 * TODO: if measuring wall-clock time then make sure there's no swapping.
 * TODO: investigate whether CounterTerminal is a bottleneck => increase parallelism + every x tuples received (and on cleanup), emit x_count (and later cleanup_count) to another Bolt that has parallelism=1 
 * TODO: maybe Kestrel tuple expiration can help when looking at data windows 
 * TODO: memory of zeroMQ
 * TODO: dynamically adding/removing queries
 * TODO: Let the user input the "query": What is the effort of detecting identical nodes in the query?
 * TODO: adjust parallelism at runtime 
 * TODO: consider more than 1 spouts
 * 
 * @author Marinos Mavrommatis
 *
 */
public class TopologyMain {

	public static final String FILTER_STREAM_PREFIX = "stream";
	public static final int KESTREL_THRIFT_PORT = 2229;
	public static final int KESTREL_MEMCACHED_PORT = 22133;
	public static final String KESTREL_IP = "localhost";
	public static final String DELIM = "_";
	public static final String VAR_INDICATOR = "?";
	public static final String FIELDS_GROUPING_VAR = "valuesAsString";
	public static final String FILTER_PREFIX = "filter";

	private static final String NO_FILTERING = "none";
	private static final String SPOUT = "kestrel-spout";
	private static final String INPUT_QUEUE = "inputQ";
	private static final String OUTPUT_QUEUE = "outputQ";
	private static final String TERMINAL_PREFIX = "terminal";
	private static final String[] SUBJECTS = new String[]{"A","B","C"};
	private static final String[] PREDICATES = new String[]{"foo","bar"};
	private static final String FILTER_DELIM = "^";
	private static final String DEFAULT_VAR = "/";
	private static final int FILTER_LENGTH = 3; //subject,predicate,object
	private static final String MULTI_JOIN_PREFIX = "multi-join";
	private static final String SIMPLE_JOIN_PREFIX = "simple-join";

	/*
	 * input size (number of tuples) = SUBJECTS.length x PREDICATES.length x NUM_OF_OBJECTS
	 */
	private static long NUM_OF_OBJECTS = 600;//500;//50000;
	/*
	 * how long to wait after submitting the topology, before killing it (in seconds)
	 */
	private static int TIME_TO_LIVE = 50;//15;//180;
	/*
	 * Whether to avoid creating identical filter Bolts (Named after "node sharing" in the Rete algorithm)
	 * If this is true, only a single filter Bolt is created that emits to all Terminal Bolts
	 * If false, multiple identical filter Bolts are created each emitting to a different Terminal Bolt
	 */
	private static enum SHARE{ALL_EMIT_EACH,ALL_EMIT_ONCE, ALL_MULTIPLE_STREAMS}; //NOTHING,SIMILAR,
	//	NOTHING("nothing"),SIMILAR("similar"),ALL_EMIT_EACH("all_emit_each"),ALL_EMIT_ONCE("all_emit_once");
	//	private String which;
	//	SHARE(String which){this.which=which;}
	private static SHARE sharing = SHARE.ALL_MULTIPLE_STREAMS;
	private static boolean shareJoins = true;
	/*
	 * Subject-Predicate-Object filter. 
	 * Parts with a '?' are variables. Others are filters.
	 * Example ?a_foo_?b => accept triples with any subject, any object but only if they have 'foo' as the predicate
	 */
	private static String[] FILTERS = new String[]{"?a_foo_?b", "A_bar_?b", "A_?c_?b", "B_?d_?b", "?e_?d_1", "?e_?d_2", "?e_?d_3"};//=36*10
	/*
	 * Specifies how many Terminal Bolts should be created.
	 * Also in case SHARING is false, this also specifies how many identical Filter nodes should be created
	 */
	private static int TERMINAL_BOLTS = 2;
	/*
	 * The parallelism_hint (initial number of executors) for each type of Bolt
	 */
	private static int SPOUT_PARALLELISM = 2;
	private static int FILTER_PARALLELISM = 1;
	private static int JOIN_PARALLELISM = 1;
	private static int TERMINAL_PARALLELISM = 1;

	private static String TOPOLOGY_NAME = "Node-Sharing-Evaluation";

	public static void main(String[] args) throws InterruptedException, TException {

		//Arguments
		for (int i=0; i<args.length; i+=2){
			if (args[i].equals("-ns")) //rename to filter sharing
				sharing = 
				//				"nothing".equals(args[i+1])? SHARE.NOTHING:
				//					"similar".equals(args[i+1])? SHARE.SIMILAR:
				"all_emit_each".equals(args[i+1])? SHARE.ALL_EMIT_EACH:
					"all_emit_once".equals(args[i+1])? SHARE.ALL_EMIT_ONCE:
						"all_multiple_streams".equals(args[i+1])? SHARE.ALL_MULTIPLE_STREAMS:
							null;
			else if (args[i].equals("-js"))
				shareJoins = Boolean.parseBoolean(args[i+1]);
			else if (args[i].equals("-F"))
				FILTERS = args[i+1].equals(NO_FILTERING) ? new String[]{} : StringUtils.split(args[i+1], FILTER_DELIM);
				else if (args[i].equals("-n"))
					TERMINAL_BOLTS = Integer.parseInt(args[i+1]);
				else if (args[i].equals("-sp"))
					SPOUT_PARALLELISM = Integer.parseInt(args[i+1]);
				else if (args[i].equals("-fp"))
					FILTER_PARALLELISM = Integer.parseInt(args[i+1]);
				else if (args[i].equals("-jp"))
					JOIN_PARALLELISM = Integer.parseInt(args[i+1]);
				else if (args[i].equals("-tp")) //FIXME: when > 1, execute() is called correctly but tuples don't always appear in the kestrel output queue  
					TERMINAL_PARALLELISM = Integer.parseInt(args[i+1]);
				else if (args[i].equals("-i"))
					NUM_OF_OBJECTS = Integer.parseInt(args[i+1]);
				else if (args[i].equals("-ttl"))
					TIME_TO_LIVE = Integer.parseInt(args[i+1]);
				else
					System.out.println("flag "+args[i]+" not recognised! Ignoring...");
		}

		//Init
		TopologyBuilder builder = new TopologyBuilder();
		String[][] FILTER_ARRAYS = new String[FILTERS.length][];
		for (int i=0; i<FILTERS.length; i++){
			String[] filterArray = StringUtils.split(FILTERS[i], DELIM);
			if (filterArray.length != FILTER_LENGTH)
				throw new IllegalArgumentException(
						"FILTER_ARRAY must be a triple of 3 strings separated by underscores");
			FILTER_ARRAYS[i]=filterArray;
		}
		if (FILTERS.length > 0)
			TERMINAL_BOLTS=FILTERS.length;
		KestrelThriftClient client = new KestrelThriftClient(KESTREL_IP, KESTREL_THRIFT_PORT);

		//Prepare input
		client.delete_queue(INPUT_QUEUE);
		for (String subj: SUBJECTS)
			for (String pred: PREDICATES)
				for (int obj=0; obj<NUM_OF_OBJECTS; obj++){
					String triple = subj +DELIM+ pred +DELIM+ obj;
					client.put(INPUT_QUEUE, triple, 0);
				}
		System.out.println("Input generated. "+INPUT_QUEUE+" contains "+
				client.peek(INPUT_QUEUE).get_items()+" items");

		//Set Spout
		KestrelThriftSpout kestrelSpout = new KestrelThriftSpout(
				KESTREL_IP, KESTREL_THRIFT_PORT, INPUT_QUEUE, new StringScheme());
		builder.setSpout(SPOUT, kestrelSpout, SPOUT_PARALLELISM);

		//Determine filters
		List<String[]> filtersToAdd;
		int[] terminalSubscriptions = new int[TERMINAL_BOLTS];

		if (sharing==null) throw new IllegalArgumentException("Illegal sharing parameter");
		//		if (sharing.equals(SHARE.SIMILAR)) {
		//
		//			/*  Filters that produce the same output are combined into a single filter,
		//			 *  even if they have different variable names */
		//
		//			filtersToAdd = new ArrayList<String[]>(FILTER_ARRAYS.length);
		//			List<String[]> correspondingVars = new ArrayList<String[]>(FILTER_ARRAYS.length);
		//
		//			for (int i=0; i<FILTER_ARRAYS.length; i++){
		//				String[] filter = FILTER_ARRAYS[i];
		//				String[] varIndependentFilter = filter.clone();
		//
		//				//remove variable names from filter clone
		//				String[] vars = new String[filter.length];
		//				for (int j=0; j<FILTER_LENGTH; j++)
		//					if (filter[j].startsWith(VAR_INDICATOR)){
		//						vars[j]=filter[j];
		//						varIndependentFilter[j]=DEFAULT_VAR;
		//					}
		//
		//				//collect any variables found in this filter
		//				int index;
		//				if ((index=deepEqualsIndexOf(filtersToAdd, varIndependentFilter)) >= 0)
		//					for (int j=0; j<FILTER_LENGTH; j++)
		//						correspondingVars.get(index)[j]+=vars[j];
		//				else{
		//					filtersToAdd.add(varIndependentFilter);
		//					correspondingVars.add(vars);
		//					index = correspondingVars.size()-1;
		//				}
		//				terminalSubscriptions[i]=index;
		//			}
		//
		//			//join filtersToAdd and correspondingVars into filterArrays
		//			for (int i=0; i<filtersToAdd.size(); i++)
		//				for (int j=0; j<FILTER_LENGTH; j++)
		//					if (filtersToAdd.get(i)[j].equals(DEFAULT_VAR))
		//						filtersToAdd.get(i)[j]=correspondingVars.get(i)[j];
		//
		//		}else{

		filtersToAdd=Arrays.asList(FILTER_ARRAYS);
		for (int i=0; i<terminalSubscriptions.length; i++)
			terminalSubscriptions[i]=i;
		//		}

		//if shareJoins
		String[][] filtersToBeGrouped = FILTER_ARRAYS.clone();
		Set<Set<Integer>> groupsOfFilters = new LinkedHashSet<Set<Integer>>(); //TODO what's the natural ordering of sets of ints??
		while(totalSize(groupsOfFilters) < FILTER_ARRAYS.length){
			// TODO: check inveriant: number of non-null entries in filtersToBeGrouped + totalSize(groupsOfFilters) should always be equal to FILTER_ARRAYS.length
			Set<Integer> selectedFilters = new TreeSet<Integer>();
			Set<String> selectedFiltersCommonVars = new TreeSet<String>();
			outter: for (int i=0; i<filtersToBeGrouped.length; i++){
				if (filtersToBeGrouped[i] == null)
					continue outter;
				Set<String> commonVars = new TreeSet<String>();
				Set<Integer> filters = new TreeSet<Integer>();
				filters.add(i);
				inner: for (int j=i+1; j<filtersToBeGrouped.length; j++){
					if (filtersToBeGrouped[j] == null)
						continue inner;
					Set<String> innerCommonVars = MultiJoinDoubleMemories.intersection(extractVars(filtersToBeGrouped[i]),extractVars(filtersToBeGrouped[j]));
					if (innerCommonVars.size() > 0 && innerCommonVars.equals(commonVars)){
						filters.add(j);
					}else if (commonVars.isEmpty() || innerCommonVars.size() > commonVars.size()){
						commonVars = innerCommonVars;
						filters = new TreeSet<Integer>();
						filters.add(i);
						filters.add(j);
					}//else continue
				}
				/*
				 * At this point, out of the filters with the BIGGEST NUMBER OF COMMON VARIABLES with the current filter, the BIGGEST SET OF FILTERS that have common variables with the current filter has been found
				 * examples of extracted group out of set of filters:
				 * 	?a_foo_?b ?b_bar_?a ?a_foo_1 => ?a_foo_?b ?a_bar_?b
				 * 	?a_foo_?c ?a_bar_?b ?a_foo_0 ?b_baar_?c => ?a_foo_?c ?a_bar_?b ?a_foo_0
				 * 
				 * The extracted group is subtracted from the original set, and the process is repeated until no filters are left 
				 */
				if (commonVars.size() > selectedFiltersCommonVars.size() || 
						commonVars.size() == selectedFiltersCommonVars.size() && filters.size() >= selectedFilters.size()){
					selectedFilters = filters;
					selectedFiltersCommonVars = commonVars;
				}
			}
			groupsOfFilters.add(selectedFilters);
			for (int filter : selectedFilters)
				filtersToBeGrouped[filter]=null;
		}

		Map<String, List<String>> fieldsGroupingVarsPerStream = new TreeMap<String, List<String>>();
		for (Set<Integer> group : groupsOfFilters){
			Set<String[]> filterGroup = extractFilterGroup(FILTER_ARRAYS,group);
			for(int filter : group)
				fieldsGroupingVarsPerStream.put(new GlobalStreamId(FILTER_PREFIX, FILTER_STREAM_PREFIX+filter).toString(), new ArrayList<String>(intersection(filterGroup)));
			//TODO: do shuffle grouping when group-size is 1 ?
		}
		//out of scope: could add dangling Cartesian filter in one of the groups. How about Cartesian between two large groups (or other order issues)

		//Set Filter bolts
		//		if (sharing.equals(SHARE.ALL_EMIT_EACH) || sharing.equals(SHARE.ALL_EMIT_ONCE) || sharing.equals(SHARE.ALL_MULTIPLE_STREAMS)){
		System.out.println("Setting 1 Filter Bolt for all(="+filtersToAdd.size()+") filters");

		IBasicBolt filterBolt = 
				//				sharing.equals(SHARE.ALL_EMIT_EACH) ? new UniversalFilterEmitEach(FILTER_ARRAYS, DELIM, VAR_INDICATOR) 
				//		: sharing.equals(SHARE.ALL_EMIT_ONCE) ? new UniversalFilterEmitOnce(FILTER_ARRAYS, DELIM, VAR_INDICATOR)
				//		: sharing.equals(SHARE.ALL_MULTIPLE_STREAMS) ? 
				new UniversalFilterMultipleStreams(FILTER_ARRAYS, fieldsGroupingVarsPerStream);
		//		: null
		;

		builder.setBolt(FILTER_PREFIX, filterBolt, FILTER_PARALLELISM)

		.shuffleGrouping(SPOUT);
		//
		//		}else{
		//			System.out.println("Setting "+filtersToAdd.size()+" Filter Bolts");
		//
		//			for (int i=0; i<filtersToAdd.size(); i++){
		//				String[] filter = filtersToAdd.get(i);
		//
		//				boolean isPassive=true;
		//				for (String part: filter)
		//					if (!part.startsWith("?0")) //using "startsWith()" because when sharing it becomes ?0?0_?0?0_?0?0
		//						isPassive=false;
		//
		//				IBasicBolt filterBolt = isPassive? 
		//						new PassThroughBolt() : 
		//							new SingleFilter(filter, DELIM, VAR_INDICATOR);
		//
		//						builder.setBolt(FILTER_PREFIX+i, filterBolt, FILTER_PARALLELISM)
		//
		//						.shuffleGrouping(SPOUT);
		//			}
		//		}

		//assuming single bolt
		GlobalStreamId last = new GlobalStreamId(FILTER_PREFIX, FILTER_STREAM_PREFIX+0); //useful when only 1 entry exists in FILTER_ARRAYS
		if (shareJoins && groupsOfFilters.size()>1){
			Iterator<Set<Integer>> iter = groupsOfFilters.iterator();
			Set<Integer> firstGroup = iter.next();
			Map<GlobalStreamId,Set<String>> firstStreamsAndVariables = new TreeMap<GlobalStreamId, Set<String>>();
			for (int filter : firstGroup){
				GlobalStreamId streamId = new GlobalStreamId(FILTER_PREFIX, FILTER_STREAM_PREFIX+filter);
				firstStreamsAndVariables.put(streamId, extractVars(FILTER_ARRAYS[filter]));
			}
			Set<Integer> secondGroup = iter.next();
			Map<GlobalStreamId,Set<String>> secondStreamsAndVariables = new TreeMap<GlobalStreamId, Set<String>>();
			for (int filter : secondGroup){
				GlobalStreamId streamId = new GlobalStreamId(FILTER_PREFIX, FILTER_STREAM_PREFIX+filter);
				secondStreamsAndVariables.put(streamId, extractVars(FILTER_ARRAYS[filter]));
			}
			Set<String> firstVarSet = unionAll(firstStreamsAndVariables.values());
			Set<String> secondVarSet = unionAll(secondStreamsAndVariables.values());
			Set<String> fieldsGroupingVars = MultiJoinDoubleMemories.intersection(firstVarSet, secondVarSet);

			GlobalStreamId penultimate = firstStreamsAndVariables.keySet().iterator().next();
			if (firstStreamsAndVariables.size() > 1){
				IBasicBolt firstMultiJoin = new MultiJoinDoubleMemories(firstStreamsAndVariables, new ArrayList<String>(fieldsGroupingVars));
				penultimate = new GlobalStreamId(MULTI_JOIN_PREFIX+0, "default");
				BoltDeclarer declarer = builder.setBolt(penultimate.get_componentId(), firstMultiJoin, JOIN_PARALLELISM);
				for (GlobalStreamId streamName : firstStreamsAndVariables.keySet())
					declarer.fieldsGrouping(streamName.get_componentId(), streamName.get_streamId(), new Fields(FIELDS_GROUPING_VAR));
			}
			last = secondStreamsAndVariables.keySet().iterator().next();
			if (secondStreamsAndVariables.size() > 1){
				IBasicBolt secondMultiJoin = new MultiJoinDoubleMemories(secondStreamsAndVariables, new ArrayList<String>(fieldsGroupingVars));
				last = new GlobalStreamId(MULTI_JOIN_PREFIX+1, "default");
				BoltDeclarer declarer = builder.setBolt(last.get_componentId(), secondMultiJoin, JOIN_PARALLELISM);
				for (GlobalStreamId streamName : secondStreamsAndVariables.keySet())
					declarer.fieldsGrouping(streamName.get_componentId(), streamName.get_streamId(), new Fields(FIELDS_GROUPING_VAR));
			}

			Set<String> unionSoFar = MultiJoinDoubleMemories.union(firstVarSet,secondVarSet);

			for (int i=2; i<groupsOfFilters.size(); i++){
				Set<Integer> currentGroup = iter.next();
				Map<GlobalStreamId,Set<String>> currentStreamsAndVariables = new TreeMap<GlobalStreamId, Set<String>>();
				for (int filter : currentGroup){
					GlobalStreamId streamId = new GlobalStreamId(FILTER_PREFIX, FILTER_STREAM_PREFIX+filter);
					currentStreamsAndVariables.put(streamId, extractVars(FILTER_ARRAYS[filter]));
				}
				Set<String> currentVarSet;
				Set<String> nextFieldsGroupingVars = MultiJoinDoubleMemories.intersection(
						unionSoFar, 
						(currentVarSet = unionAll(currentStreamsAndVariables.values())));

				//join the previously created SimpleJoin and MultiJoin
				String boltName = SIMPLE_JOIN_PREFIX+(i-1);
				builder.setBolt(boltName, new SimpleJoinDoubleMemory(
						penultimate.toString(), 
						last.toString(), 
						fieldsGroupingVars, new ArrayList<String>(nextFieldsGroupingVars)), JOIN_PARALLELISM)
						.fieldsGrouping(penultimate.get_componentId(), penultimate.get_streamId(), new Fields(FIELDS_GROUPING_VAR))
						.fieldsGrouping(last.get_componentId(), last.get_streamId(), new Fields(FIELDS_GROUPING_VAR));
				penultimate = new GlobalStreamId(boltName, "default");

				last = currentStreamsAndVariables.keySet().iterator().next();
				if (currentStreamsAndVariables.size() > 1){
					//create new MultiJoin but don't join it yet
					IBasicBolt currentJoinBolt = new MultiJoinDoubleMemories(currentStreamsAndVariables, new ArrayList<String>(fieldsGroupingVars));
					last = new GlobalStreamId(MULTI_JOIN_PREFIX+i, "default");
					BoltDeclarer currentDeclarer = builder.setBolt(last.get_componentId(), currentJoinBolt, JOIN_PARALLELISM);
					for (GlobalStreamId streamName : currentStreamsAndVariables.keySet())
						currentDeclarer.fieldsGrouping(streamName.get_componentId(), streamName.get_streamId(), new Fields(FIELDS_GROUPING_VAR));
				}

				fieldsGroupingVars = nextFieldsGroupingVars;
				unionSoFar = MultiJoinDoubleMemories.union(unionSoFar, currentVarSet);
			}
			IBasicBolt joinBolt = new SimpleJoinDoubleMemory(
					penultimate.toString(), 
					last.toString(),
					fieldsGroupingVars, new ArrayList<String>(0));

			String boltName = SIMPLE_JOIN_PREFIX+(groupsOfFilters.size()-1);
			builder.setBolt(boltName, joinBolt, JOIN_PARALLELISM)
			.fieldsGrouping(penultimate.get_componentId(), penultimate.get_streamId(), new Fields(FIELDS_GROUPING_VAR))
			.fieldsGrouping(last.get_componentId(), last.get_streamId(), new Fields(FIELDS_GROUPING_VAR));
			
			last = new GlobalStreamId(boltName, "default");

		}else if (FILTER_ARRAYS.length > 1){

			GlobalStreamId leftInput = new GlobalStreamId(FILTER_PREFIX, FILTER_STREAM_PREFIX+0);
			GlobalStreamId rightInput = new GlobalStreamId(FILTER_PREFIX, FILTER_STREAM_PREFIX+1);

			List<String> fieldsGroupingVars = FILTER_ARRAYS.length == 2 ? new ArrayList<String>(0) :
				new ArrayList<String>(MultiJoinDoubleMemories.intersection(unionVarsUpTo(FILTER_ARRAYS, 1), extractVars(FILTER_ARRAYS[2])));

			IBasicBolt joinBolt = new SimpleJoinDoubleMemory(leftInput.toString(), rightInput.toString(), 
					MultiJoinDoubleMemories.intersection(extractVars(FILTER_ARRAYS[0]), extractVars(FILTER_ARRAYS[1])), fieldsGroupingVars);

			builder.setBolt(MULTI_JOIN_PREFIX+1, joinBolt, JOIN_PARALLELISM) 
			.fieldsGrouping(leftInput.get_componentId(), leftInput.get_streamId(), new Fields(FIELDS_GROUPING_VAR))
			.fieldsGrouping(rightInput.get_componentId(), rightInput.get_streamId(), new Fields(FIELDS_GROUPING_VAR));

			for (int i=2; i<FILTER_ARRAYS.length; i++){
				leftInput = new GlobalStreamId(MULTI_JOIN_PREFIX+(i-1), "default");
				rightInput = new GlobalStreamId(FILTER_PREFIX, FILTER_STREAM_PREFIX+i);

				fieldsGroupingVars = i == FILTER_ARRAYS.length-1 ? new ArrayList<String>(0) :
					new ArrayList<String>(MultiJoinDoubleMemories.intersection(unionVarsUpTo(FILTER_ARRAYS, i), extractVars(FILTER_ARRAYS[i+1])));

				joinBolt = new SimpleJoinDoubleMemory(leftInput.toString(), rightInput.toString(),
						MultiJoinDoubleMemories.intersection(unionVarsUpTo(FILTER_ARRAYS, i-1), extractVars(FILTER_ARRAYS[i])), fieldsGroupingVars);

				builder.setBolt(MULTI_JOIN_PREFIX+i, joinBolt, JOIN_PARALLELISM)
				.fieldsGrouping(leftInput.get_componentId(), leftInput.get_streamId(), new Fields(FIELDS_GROUPING_VAR))
				.fieldsGrouping(rightInput.get_componentId(), rightInput.get_streamId(), new Fields(FIELDS_GROUPING_VAR));
			}

			last = new GlobalStreamId(MULTI_JOIN_PREFIX+(FILTER_ARRAYS.length-1), "default");
		}

		//Set Terminal bolts
		builder.setBolt(TERMINAL_PREFIX, new CounterTerminal(0), TERMINAL_PARALLELISM)
		.shuffleGrouping(last.get_componentId(), last.get_streamId());
		//		if (sharing.equals(SHARE.ALL_EMIT_EACH) || 
		//				sharing.equals(SHARE.ALL_EMIT_ONCE) || 
		//				sharing.equals(SHARE.ALL_MULTIPLE_STREAMS)){
		//
		//			for (int i=0; i<FILTER_ARRAYS.length; i++){
		//
		//				IBasicBolt terminalBolt = sharing.equals(SHARE.ALL_EMIT_EACH)? new CounterTerminalReceiveEach(i)
		//				: sharing.equals(SHARE.ALL_EMIT_ONCE)? new CounterTerminalReceivePack(i) 
		//				: new CounterTerminal(i);
		//
		//				BoltDeclarer terminalDeclarer = builder.setBolt(TERMINAL_PREFIX+i, terminalBolt, TERMINAL_PARALLELISM);
		//
		//				if (sharing.equals(SHARE.ALL_MULTIPLE_STREAMS))
		//					terminalDeclarer.shuffleGrouping(FILTER_PREFIX, FILTER_STREAM_PREFIX+i);
		//				else
		//					terminalDeclarer.shuffleGrouping(FILTER_PREFIX);
		//
		//				//Determine expected result size FIXME: will give wrong value for "?a_foo_?a"
		//				final long[] sizes = new long[]{SUBJECTS.length, PREDICATES.length, NUM_OF_OBJECTS};
		//				long expectedResultSize = 1;
		//				for (int j=0; j<FILTER_LENGTH; j++)
		//					if (FILTER_ARRAYS[i][j].startsWith(VAR_INDICATOR))
		//						expectedResultSize*=sizes[j];
		//				System.out.println("ExpectedResultSize"+i+'='+expectedResultSize);
		//			}
		//		}else{
		//
		//			if (FILTER_ARRAYS.length == 0)
		//				for (int i=0; i<terminalSubscriptions.length; i++)
		//					terminalSubscriptions[i]=-1;
		//
		//			System.out.println("Setting "+terminalSubscriptions.length+" CounterTerminal Bolts");
		//
		//			for (int i=0; i<terminalSubscriptions.length; i++){
		//
		//				IBasicBolt terminalBolt = new ComplexCounterTerminal(i);
		//
		//				BoltDeclarer declarer=builder.setBolt(TERMINAL_PREFIX+i, terminalBolt, TERMINAL_PARALLELISM);
		//
		//				if (terminalSubscriptions[i]==-1)
		//					declarer.shuffleGrouping(SPOUT);
		//				else
		//					declarer.shuffleGrouping(FILTER_PREFIX+terminalSubscriptions[i]);
		//
		//				//Determine expected result size FIXME: will give wrong value for "?a_foo_?a". btw if fixed it will give wrong value for ?0_?0_?0
		//				final long[] sizes = new long[]{SUBJECTS.length, PREDICATES.length, NUM_OF_OBJECTS};
		//				long expectedResultSize = 1;
		//				for (int j=0; j<FILTER_LENGTH; j++)
		//					if (terminalSubscriptions[i]==-1 || FILTER_ARRAYS[i][j].startsWith(VAR_INDICATOR))
		//						expectedResultSize*=sizes[j];
		//				System.out.println("ExpectedResultSize"+i+'='+expectedResultSize);
		//			}
		//		}

		//Configure
		Config conf = new Config();
		conf.setDebug(true);
		//conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1); //TODO understand

		//Run Topology
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
		Utils.sleep(TIME_TO_LIVE*1000);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();

		//check whether output queues contain the expected number of tuples
		/* currently commented-out because using CounterTerminal instead
		boolean resultsOK = true;
		for (int i = 0; i < NUM_OF_IDENTICAL_NODES; i++){
			long items = client.peek(OUTPUT_QUEUE+i).get_items();
			System.out.println(OUTPUT_QUEUE+i+" contains "+items+" items");
			if (items != expectedResultSize)
				resultsOK=false;
		}
		System.out.println("Results_OK="+resultsOK);
		 */

		client.close();
	}

	private static Map<Integer,String[]> toTreeMap(String[][] FILTER_ARRAYS) {
		Map<Integer,String[]> map = new TreeMap<Integer,String[]>();
		for (int i=0; i<FILTER_ARRAYS.length; i++)
			map.put(i, FILTER_ARRAYS[i]);
		return map;
	}

	private static Set<String[]> extractFilterGroup(String[][] FILTER_ARRAYS, Set<Integer> group) {
		Set<String[]> filters = new LinkedHashSet<String[]>();
		for (int i : group){
			String[] foo = FILTER_ARRAYS[i];
			filters.add(foo);
		}
		return filters;
	}

	private static Set<String> intersection(Set<String[]> group) {
		Set<String> intersection = new TreeSet<String>();
		Iterator<String[]> iter = group.iterator();
		intersection.addAll(extractVars(iter.next())); //assumes there exists at least one group filter in "group"
		while (iter.hasNext())
			intersection.retainAll(extractVars(iter.next()));
		return intersection;
	}

	private static Set<String> unionAll(Collection<Set<String>> sets) {
		Set<String> union = new TreeSet<String>();
		for (Set<String> set : sets)
			union.addAll(set);
		return union;
	}

	private static Set<String> extractVars(String[] array) {
		Set<String> vars = new TreeSet<String>();
		for (String part : array)
			if (part.startsWith(VAR_INDICATOR))
				vars.add(part);
		return vars;
	}

	private static int totalSize(Set<Set<Integer>> groupsOfFilters) {
		int sum = 0;
		for (Set<Integer> group : groupsOfFilters)
			sum+=group.size();
		return sum;
	}

	private static Set<String> unionVarsUpTo(String[][] filters, int lastIndex) { //TODO use libraries
		Set<String> union = new TreeSet<String>();
		for (int i=0; i<=lastIndex; i++)
			for (String vars : extractVars(filters[i]))
				union.add(vars);
//		String[] result = new String[union.size()];
//		int i=0;
//		for (String s : union)
//			result[i++] = s;

		return union;
	}

	private static int deepEqualsIndexOf(List<String[]> haystack, String[] needle) {
		for (int i=0; i<haystack.size(); i++){
			boolean equal = true;
			for (int j=0; j<FILTER_LENGTH; j++){
				if (!haystack.get(i)[j].equals(needle[j])){
					equal = false;
				}
			}
			if (equal) return i;
		}
		return -1;
	}

	private static Set<String> intersection(String[] left, String[] right){ //TODO just call extractVars in here to avoid doing all the time
		Set<String> intersection = new TreeSet<String>();
		for (String varName : left){ //TODO replace with more readable "intersection" library method
			for (String prevFilterVarName : right){
				if (varName.equals(prevFilterVarName)){
					intersection.add(varName);
				}
			}
		}
		return intersection;
	}

	//	private static List<String> asList(Set<String> set){
	//		List<String> list = new ArrayList<String>(set.size());
	//		list.addAll(set);
	//		return list;
	//	}
}
