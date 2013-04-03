package org.reteonstorm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift7.TException;
import org.reteonstorm.bolts.ComplexCounterTerminal;
import org.reteonstorm.bolts.CounterTerminalReceiveEach;
import org.reteonstorm.bolts.CounterTerminalReceivePack;
import org.reteonstorm.bolts.PassThroughBolt;
import org.reteonstorm.bolts.SingleFilter;
import org.reteonstorm.bolts.UniversalFilterEmitEach;
import org.reteonstorm.bolts.UniversalFilterEmitOnce;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.KestrelThriftClient;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.TopologyBuilder;
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
	private static final String NO_FILTERING = "none";
	private static final String SPOUT = "kestrel-spout";
	private static final String INPUT_QUEUE = "inputQ";
	private static final String OUTPUT_QUEUE = "outputQ";
	public static final int KESTREL_THRIFT_PORT = 2229;
	public static final int KESTREL_MEMCACHED_PORT = 22133;
	public static final String KESTREL_IP = "localhost";
	private static final String TERMINAL_PREFIX = "terminal";
	private static final String FILTER_PREFIX = "filter";
	private static final String[] SUBJECTS = new String[]{"A","B","C"};
	private static final String[] PREDICATES = new String[]{"foo","bar"};
	private static final String DELIM = "_";
	private static final String FILTER_DELIM = "^";
	private static final String VAR_INDICATOR = "?";
	private static final String DEFAULT_VAR = "/";
	private static final int FILTER_LENGTH = 3; //subject,predicate,object

	/*
	 * input size (number of tuples) = SUBJECTS.length x PREDICATES.length x NUM_OF_OBJECTS
	 */
	private static long NUM_OF_OBJECTS = 50000;//50000;
	/*
	 * how long to wait after submitting the topology, before killing it (in seconds)
	 */
	private static int TIME_TO_LIVE = 500;//180;
	/*
	 * Whether to avoid creating identical filter Bolts (Named after "node sharing" in the Rete algorithm)
	 * If this is true, only a single filter Bolt is created that emits to all Terminal Bolts
	 * If false, multiple identical filter Bolts are created each emitting to a different Terminal Bolt
	 */
	private static enum SHARE{NOTHING,SIMILAR,ALL_EMIT_EACH,ALL_EMIT_ONCE};
	//	NOTHING("nothing"),SIMILAR("similar"),ALL_EMIT_EACH("all_emit_each"),ALL_EMIT_ONCE("all_emit_once");
	//	private String which;
	//	SHARE(String which){this.which=which;}
	private static SHARE sharing = SHARE.NOTHING;
	/*
	 * Subject-Predicate-Object filter. 
	 * Parts with a '?' are variables. Others are filters.
	 * Example ?a_foo_?b => accept triples with any subject, any object but only if they have 'foo' as the predicate
	 */
	private static String[] FILTERS = new String[]{"?a_foo_?b", "?a_bar_?b"};
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
	private static int TERMINAL_PARALLELISM = 1;

	private static String TOPOLOGY_NAME = "Node-Sharing-Evaluation";

	public static void main(String[] args) throws InterruptedException, TException {

		//Arguments
		for (int i=0; i<args.length; i+=2){
			if (args[i].equals("-ns"))
				sharing = "nothing".equals(args[i+1])? SHARE.NOTHING:
					"similar".equals(args[i+1])? SHARE.SIMILAR:
						"all_emit_each".equals(args[i+1])? SHARE.ALL_EMIT_EACH:
							"all_emit_once".equals(args[i+1])? SHARE.ALL_EMIT_ONCE:null;
			else if (args[i].equals("-F"))
				FILTERS = args[i+1].equals(NO_FILTERING) ? new String[]{} : StringUtils.split(args[i+1], FILTER_DELIM);
				else if (args[i].equals("-n"))
					TERMINAL_BOLTS = Integer.parseInt(args[i+1]);
				else if (args[i].equals("-sp"))
					SPOUT_PARALLELISM = Integer.parseInt(args[i+1]);
				else if (args[i].equals("-fp"))
					FILTER_PARALLELISM = Integer.parseInt(args[i+1]);
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
		if (sharing.equals(SHARE.SIMILAR)) {

			/*  Filters that produce the same output are combined into a single filter,
			 *  even if they have different variable names */

			filtersToAdd = new ArrayList<String[]>(FILTER_ARRAYS.length);
			List<String[]> correspondingVars = new ArrayList<String[]>(FILTER_ARRAYS.length);

			for (int i=0; i<FILTER_ARRAYS.length; i++){
				String[] filter = FILTER_ARRAYS[i];
				String[] varIndependentFilter = filter.clone();

				//remove variable names from filter clone
				String[] vars = new String[filter.length];
				for (int j=0; j<FILTER_LENGTH; j++)
					if (filter[j].startsWith(VAR_INDICATOR)){
						vars[j]=filter[j];
						varIndependentFilter[j]=DEFAULT_VAR;
					}

				//collect any variables found in this filter
				int index;
				if ((index=deepEqualsIndexOf(filtersToAdd, varIndependentFilter)) >= 0)
					for (int j=0; j<FILTER_LENGTH; j++)
						correspondingVars.get(index)[j]+=vars[j];
				else{
					filtersToAdd.add(varIndependentFilter);
					correspondingVars.add(vars);
					index = correspondingVars.size()-1;
				}
				terminalSubscriptions[i]=index;
			}

			//join filtersToAdd and correspondingVars into filterArrays
			for (int i=0; i<filtersToAdd.size(); i++)
				for (int j=0; j<FILTER_LENGTH; j++)
					if (filtersToAdd.get(i)[j].equals(DEFAULT_VAR))
						filtersToAdd.get(i)[j]=correspondingVars.get(i)[j];

		}else{

			filtersToAdd=Arrays.asList(FILTER_ARRAYS);
			for (int i=0; i<terminalSubscriptions.length; i++)
				terminalSubscriptions[i]=i;
		}

		//Set FilterBolts
		if (sharing.equals(SHARE.ALL_EMIT_EACH) || sharing.equals(SHARE.ALL_EMIT_ONCE)){
			System.out.println("Setting 1 Filter Bolt for all(="+filtersToAdd.size()+") filters");

			IBasicBolt filterBolt = sharing.equals(SHARE.ALL_EMIT_EACH) ?
					new UniversalFilterEmitEach(FILTER_ARRAYS, DELIM, VAR_INDICATOR) :
						new UniversalFilterEmitOnce(FILTER_ARRAYS, DELIM, VAR_INDICATOR);

					builder.setBolt(FILTER_PREFIX, filterBolt, FILTER_PARALLELISM)

					.shuffleGrouping(SPOUT);

		}else{
			System.out.println("Setting "+filtersToAdd.size()+" Filter Bolts");

			for (String[] filter : filtersToAdd){

				boolean isPassive=true;
				for (String part: filter)
					if (!part.equals("?0"))
						isPassive=false;

				IBasicBolt filterBolt = isPassive? 
						new PassThroughBolt() : 
							new SingleFilter(filter, DELIM, VAR_INDICATOR);

						builder.setBolt(FILTER_PREFIX+filter.toString(), filterBolt, FILTER_PARALLELISM)

						.shuffleGrouping(SPOUT);
			}
		}

		//Set TerminalBolts
		if (sharing.equals(SHARE.ALL_EMIT_EACH) || sharing.equals(SHARE.ALL_EMIT_ONCE)){

			for (int i=0; i<FILTER_ARRAYS.length; i++){
				IBasicBolt terminalBolt = sharing.equals(SHARE.ALL_EMIT_EACH)?
						new CounterTerminalReceiveEach(i):
							new CounterTerminalReceivePack(i);
						builder.setBolt(TERMINAL_PREFIX+i, terminalBolt, TERMINAL_PARALLELISM)
						.shuffleGrouping(FILTER_PREFIX);

						//Determine expected result size FIXME: will give wrong value for "?a_foo_?a"
						final long[] sizes = new long[]{SUBJECTS.length, PREDICATES.length, NUM_OF_OBJECTS};
						long expectedResultSize = 1;
						for (int j=0; j<FILTER_LENGTH; j++)
							if (FILTER_ARRAYS[i][j].startsWith(VAR_INDICATOR))
								expectedResultSize*=sizes[j];
						System.out.println("ExpectedResultSize"+i+'='+expectedResultSize);
			}
		}else{

			if (FILTER_ARRAYS.length == 0)
				for (int i=0; i<terminalSubscriptions.length; i++)
					terminalSubscriptions[i]=-1;

			System.out.println("Setting "+terminalSubscriptions.length+" CounterTerminal Bolts");

			for (int i=0; i<terminalSubscriptions.length; i++){

				//			List<String> expectedVars = new ArrayList<String>(FILTER_LENGTH);
				//				for (String part: filtersToAdd.get(i))
				//					if (part.startsWith(VAR_INDICATOR))
				//						expectedVars.add(part.substring(VAR_INDICATOR.length()));

				IBasicBolt terminalBolt = new ComplexCounterTerminal(i);

				BoltDeclarer declarer=builder.setBolt(TERMINAL_PREFIX+i, terminalBolt, TERMINAL_PARALLELISM);

				if (terminalSubscriptions[i]==-1)
					declarer.shuffleGrouping(SPOUT);
				else
					declarer.shuffleGrouping(FILTER_PREFIX+filtersToAdd.get(terminalSubscriptions[i]).toString());

				//Determine expected result size FIXME: will give wrong value for "?a_foo_?a"
				final long[] sizes = new long[]{SUBJECTS.length, PREDICATES.length, NUM_OF_OBJECTS};
				long expectedResultSize = 1;
				for (int j=0; j<FILTER_LENGTH; j++)
					if (terminalSubscriptions[i]==-1 || FILTER_ARRAYS[i][j].startsWith(VAR_INDICATOR))
						expectedResultSize*=sizes[j];
				System.out.println("ExpectedResultSize"+i+'='+expectedResultSize);
			}
		}

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
}