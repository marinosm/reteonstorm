package org.reteonstorm;

import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift7.TException;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.KestrelThriftClient;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * This is to test whether there is measurable benefit in avoiding to create multiple identical Storm Bolts
 * 
 * TODO: add support for cross-products => need to detect and use allGrouping (e.g. ?a_foo_?b, ?c_bar_1)
 * TODO: Add support for filter-after-filter if useful
 * 
 * TODO: does increasing parallelism (when using fieldsGrouping) decrease the memory requirements (window size) of each Bolt instance?
 * 
 * TODO: would be nice to measure lines in log file to confirm that actual number of messages increase/decrease
 * TODO: would be nice to use an output queue to which all instances of the terminal bolt output triples to (a quick investigation showed that while all instances were emiting tuples, kestrel only saw those coming from one instance)
 * TODO: tune parallelism on real cluster (including "parallelism_hint, number of tasks etc." both before deploying and at runtime)
 * 
 * TODO: time windows: kestrel&Storm tuple expiration time 
 * TODO: dynamically adding/removing queries
 * 
 * @author Marinos Mavrommatis
 *
 */
public class TopologyMain {

	public static final String INPUT_QUEUE = "inputQ";
	public static final String OUTPUT_QUEUE = "outputQ";
	public static final int KESTREL_THRIFT_PORT = 2229;
	public static final int KESTREL_MEMCACHED_PORT = 22133;
	public static final String KESTREL_IP = "localhost";
	public static final String DELIM = "_";
	public static final String VAR_INDICATOR = "?";
	public static final String FILTER_DELIM = "^";
	public static final String DEFAULT_VAR = "/";
	public static final int FILTER_LENGTH = 3; //subject,predicate,object
	public static final String SPOUT_NAME = "kestrel-spout";
	public static final String FILTER_PREFIX = "filter";
	public static final String MULTI_JOIN_PREFIX = "multi-join";
	public static final String SIMPLE_JOIN_PREFIX = "simple-join";
	public static final String TERMINAL_PREFIX = "terminal";
	public static final String DEFAULT_STREAM_NAME = "default";
	public static final String FILTER_STREAM_PREFIX = "stream";
	public static final String NO_FILTERING = "none";
	public static final String FIELDS_GROUPING_VAR = "valuesAsString";
	public static final String[] SUBJECTS = new String[]{"A","B","C"};
	public static final String[] PREDICATES = new String[]{"foo","bar"};

	/*
	 * input size (number of tuples) = SUBJECTS.length x PREDICATES.length x NUM_OF_OBJECTS
	 */
	protected static long NUM_OF_OBJECTS = 600;//500;//50000;
	/*
	 * how long to wait after submitting the topology, before killing it (in seconds)
	 */
	protected static int TIME_TO_LIVE = 20;//15;//180;
	/*
	 * Whether to avoid creating identical filter Bolts (Named after "node sharing" in the Rete algorithm)
	 * If this is true, only a single filter Bolt is created that emits to all Terminal Bolts
	 * If false, multiple identical filter Bolts are created each emitting to a different Terminal Bolt
	 */
	public static enum SHARE{ALL_EMIT_EACH,ALL_EMIT_ONCE, ALL, SIMILAR, NOTHING}; //NOTHING,SIMILAR,
	//	NOTHING("nothing"),SIMILAR("similar"),ALL_EMIT_EACH("all_emit_each"),ALL_EMIT_ONCE("all_emit_once");
	//	private String which;
	//	SHARE(String which){this.which=which;}
	protected static SHARE filterSharing = SHARE.NOTHING;
	protected static boolean joinSharing = true;
	/*
	 * Subject-Predicate-Object filter. 
	 * Parts with a '?' are variables. Others are filters.
	 * Example ?a_foo_?b => accept triples with any subject, any object but only if they have 'foo' as the predicate
	 */
	protected static String[] FILTERS = new String[]{"?a_foo_?b", "A_bar_?b", "A_?c_?b", "B_?d_?b", "?e_?d_1", "?e_?d_2", "?e_?d_3"};//=36*10
	/*
	 * Specifies how many Terminal Bolts should be created.
	 * Also in case SHARING is false, this also specifies how many identical Filter nodes should be created
	 */
	protected static short TERMINAL_BOLTS = 2;
	/*
	 * The parallelism_hint (initial number of executors) for each type of Bolt
	 */
	protected static int SPOUT_PARALLELISM = 2;
	public static int FILTER_PARALLELISM = 1;
	public static int JOIN_PARALLELISM = 1;
	public static int TERMINAL_PARALLELISM = 1;

	protected static String TOPOLOGY_NAME = "Node-Sharing-Evaluation";

	public static void main(String[] args) throws InterruptedException, TException {

		//Arguments
		for (int i=0; i<args.length; i+=2){
			if (args[i].equals("-ns")){ //rename to filter sharing
				filterSharing = 
						"nothing".equals(args[i+1])? SHARE.NOTHING:
							"similar".equals(args[i+1])? SHARE.SIMILAR:
								"all_multiple_streams".equals(args[i+1])? SHARE.ALL:
									null;
			}else if (args[i].equals("-js"))
				joinSharing = Boolean.parseBoolean(args[i+1]);
			else if (args[i].equals("-F")){
				FILTERS = args[i+1].equals(NO_FILTERING) ? new String[]{} : StringUtils.split(args[i+1], FILTER_DELIM);
			}else if (args[i].equals("-n"))
				TERMINAL_BOLTS = Short.parseShort(args[i+1]);
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
		if (filterSharing==null) throw new IllegalArgumentException("Illegal sharing parameter");
		TopologyBuilder builder = new TopologyBuilder();
		String[][] FILTER_ARRAYS = new String[FILTERS.length][];
		for (int i=0; i<FILTERS.length; i++){
			String[] filterArray = StringUtils.split(FILTERS[i], DELIM);
			if (filterArray.length != FILTER_LENGTH)
				throw new IllegalArgumentException("FILTER_ARRAY must be a triple of 3 strings separated by underscores");
			FILTER_ARRAYS[i]=filterArray;
		}
		if (FILTERS.length > 0) //desirable number of terminal bolts might be different than filterArrays.length (eg when testing direct spout to terminal messages)
			TERMINAL_BOLTS = (short) FILTERS.length; 
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
		builder.setSpout(SPOUT_NAME, kestrelSpout, SPOUT_PARALLELISM);

		//Set Bolts
		if (filterSharing.equals(SHARE.ALL)){
			Set<Set<Integer>> groupsOfFilters = Algorithms.groupFilters(FILTER_ARRAYS);
			GlobalStreamId[] streams = BoltAdder.addSingleFilter(builder, FILTER_ARRAYS, groupsOfFilters);
			GlobalStreamId lastStream = BoltAdder.addJoins(builder, FILTER_ARRAYS, streams, joinSharing, groupsOfFilters);
			BoltAdder.addSingleCounterTerminal(builder, FILTER_ARRAYS, lastStream);
			System.out.println("ExpectedResultSize0="+Algorithms.expectedResultSize(FILTER_ARRAYS, NUM_OF_OBJECTS));
		}else{
			GlobalStreamId[] streams = BoltAdder.addFilters(builder, FILTER_ARRAYS, filterSharing.equals(SHARE.SIMILAR));
			BoltAdder.addCounterTerminals(builder, FILTER_ARRAYS, streams);
			long[] expectedResultSizes = Algorithms.separateExpectedResultSizes(FILTER_ARRAYS, NUM_OF_OBJECTS);
			for (int i=0; i<expectedResultSizes.length; i++)
				System.out.println("ExpectedResultSize"+i+"="+expectedResultSizes[i]);
		}

		//Configure
		Config conf = new Config();
		conf.setDebug(true);

		//Run Topology
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
		Utils.sleep(TIME_TO_LIVE*1000);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();

		//check whether output queues contain the expected number of tuples
		/* currently commented-out because using CounterTerminal instead
		long items = client.peek(OUTPUT_QUEUE).get_items();
		 */

		client.close();
	}
}
