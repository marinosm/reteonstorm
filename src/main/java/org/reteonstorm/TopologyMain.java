package org.reteonstorm;

import java.util.ArrayList;
import java.util.List;
import org.apache.thrift7.TException;
import org.reteonstorm.bolts.CounterTerminal;
import org.reteonstorm.bolts.KestrelTerminal;
import org.reteonstorm.bolts.TripleFilter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.KestrelThriftClient;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * This is to test whether there is measurable benefit in avoiding to create multiple identical Storm Bolts
 * (for now the identical filter Bolts consume identical input)
 * 
 * TODO: what if identical Bolts but different inputs
 * TODO: how about Joins?
 * TODO: experiment with Tasks as well. (Each thread calling one exec method and then another)
 * TODO: Fields grouping necessary for certain operations. In that case maybe higher parallelism is necessary
 * TODO: use various groupings to subscribe to the shared/duplicated node
 * TODO: if measuring wall-clock time then make sure there's no swapping.
 * TODO: investigate whether CounterTerminal is a bottleneck => increase parallelism + every x tuples received (and on cleanup), emit x_count (and later cleanup_count) to another Bolt that has parallelism=1 
 * TODO: maybe Kestrel tuple expiration can help when looking at data windows 
 * TODO: memory of zeroMQ
 * TODO: dynamically adding/removing queries
 * TODO: consider more than 1 spouts
 * TODO: Let the user input the "query": What is the effort of detecting identical nodes in the query?
 * TODO: re-take measurements on real Cluster (code changes?)
 * 
 * @author Marinos Mavrommatis
 *
 */
public class TopologyMain {
	private static final String KESTREL_SPOUT = "kestrel-spout";
	private static final String INPUT_QUEUE = "inputQ";
	private static final String OUTPUT_QUEUE = "outputQ";
	public static final int KESTREL_THRIFT_PORT = 2229;
	public static final int KESTREL_MEMCACHED_PORT = 22133;
	public static final String KESTREL_IP = "localhost";
	private static final String TERMINAL_NAME = "terminal";
	private static final String FILTER_NAME = "filter";
	private static final String INPUT_FILE_CONFIG = "inputFile";
	private static final String[] SUBJECTS = new String[]{"A","B","C"};
	private static final String[] PREDICATES = new String[]{"foo","bar"};
	private static final String DELIM = "_";
	private static final char VAR_INDICATOR = '?';

	/*
	 * input size (number of tuples) = SUBJECTS.length x PREDICATES.length x NUM_OF_OBJECTS
	 */
	private static long NUM_OF_OBJECTS = 1;//50000;
	/*
	 * how long to wait after submitting the topology, before killing it (in seconds)
	 */
	private static int TIME_TO_LIVE = 8;//180;
	/*
	 * Whether to avoid creating identical filter Bolts (Named after "node sharing" in the Rete algorithm)
	 * If this is true, only a single filter Bolt is created that emits to all Terminal Bolts
	 * If false, multiple identical filter Bolts are created each emitting to a different Terminal Bolt
	 */
	private static boolean SHARING = true;
	/*
	 * Subject-Predicate-Object filter. 
	 * Parts with a '?' are variables. Others are filters.
	 * Example ?a_foo_?b => accept triples with any subject, any object but only if they have 'foo' as the predicate
	 */
	private static String FILTER = "?a_foo_0";
	private static String[] FILTER_ARRAY;
	/*
	 * Specifies how many Terminal Bolts should be created.
	 * Also in case SHARING is false, this also specifies how many identical Filter nodes should be created
	 */
	private static int NUM_OF_IDENTICAL_NODES = 2;
	/*
	 * The parallelism_hint (initial number of executors) for each type of Bolt
	 */
	private static int SPOUT_PARALLELISM = 2;
	private static int FILTER_PARALLELISM = 2;
	private static int TERMINAL_PARALLELISM = 3;

	private static String TOPOLOGY_NAME = "Node-Sharing-Evaluation";

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException, TException {

		//Arguments
		for (int i=0; i<args.length; i+=2){
			if (args[i].equals("-n"))
				NUM_OF_IDENTICAL_NODES = Integer.parseInt(args[i+1]);
			else if (args[i].equals("-ns"))
				SHARING = Boolean.parseBoolean(args[i+1]);
			else if (args[i].equals("-F"))
				FILTER = args[i+1];
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

		//init
		TopologyBuilder builder = new TopologyBuilder();
		FILTER_ARRAY = FILTER.split(DELIM);
		if (FILTER_ARRAY.length != 3)
			throw new IllegalArgumentException(
					"FILTER_ARRAY must be a triple of 3 strings separated by underscores");
		KestrelThriftClient client = new KestrelThriftClient(KESTREL_IP, KESTREL_THRIFT_PORT);

		//prepare input
		client.delete_queue(INPUT_QUEUE);
		for (String subj: SUBJECTS)
			for (String pred: PREDICATES)
				for (int obj=0; obj<NUM_OF_OBJECTS; obj++){
					String triple = subj +DELIM+ pred +DELIM+ obj;
					client.put(INPUT_QUEUE, triple, 0);
				}
		System.out.println("Input generated. "+INPUT_QUEUE+" contains "+
				client.peek(INPUT_QUEUE).get_items()+" items");

		//Spout
		KestrelThriftSpout kestrelSpout = new KestrelThriftSpout(
				KESTREL_IP, KESTREL_THRIFT_PORT, INPUT_QUEUE, new StringScheme());
		builder.setSpout(KESTREL_SPOUT, kestrelSpout, SPOUT_PARALLELISM);

		//filter-Bolt(s)
		Object filterName;
		if (SHARING) { 
			/* create a single filter-Bolt.
			 * Receives tuples from the kestrel-Spout and emits to all terminal-Bolts */
			filterName = FILTER_NAME;

			IBasicBolt filterBolt = new TripleFilter(FILTER_ARRAY, DELIM, VAR_INDICATOR);

			builder.setBolt((String)filterName, filterBolt, FILTER_PARALLELISM)
			.shuffleGrouping(KESTREL_SPOUT);

		} else {
			/* create NUM_OF_IDENTICAL_NODES filterBolts.
			 * all subscribe to the same kestrel-Spout, but each sends to a different terminal-Bolt */
			filterName = new ArrayList<String>(NUM_OF_IDENTICAL_NODES);

			for (int i = 0; i < NUM_OF_IDENTICAL_NODES; i++){
				String currentName = FILTER_NAME+"-"+i;
				((List<String>) filterName).add(currentName);

				IBasicBolt filterBolt = new TripleFilter(FILTER_ARRAY, DELIM, VAR_INDICATOR);

				builder.setBolt(currentName, filterBolt, FILTER_PARALLELISM)
				.shuffleGrouping(KESTREL_SPOUT);
			}
		}

		//terminal-Bolts
		String currentFilter = SHARING? (String)filterName : null;
		for (int i = 0; i < NUM_OF_IDENTICAL_NODES; i++){
			/* create NUM_OF_IDENTICAL_NODES Terminals. 
			 * if SHARING, all subscribe to the only filter-Bolt created
			 * otherwise each subscribes to a different filter-Bolt */

			if (!SHARING)
				currentFilter = ((List<String>)filterName).remove(0);

			/* The KestrelTerminal
			String queueName = OUTPUT_QUEUE+i;
			client.delete_queue(queueName);
			builder.setBolt(TERMINAL_NAME+"-"+i, new KestrelTerminal(queueName), TERMINAL_PARALLELISM)
			*/

			builder.setBolt(TERMINAL_NAME+"-"+i, new CounterTerminal(i), TERMINAL_PARALLELISM)
			.shuffleGrouping(currentFilter); //TODO: .fieldsGrouping(currentFilter, new Fields("value=a|b")); 
		}

		//Configuration
		Config conf = new Config();
		conf.setDebug(true);
		//conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1); //TODO understand

		//Topology run
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
		Utils.sleep(TIME_TO_LIVE*1000);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();

		//determine expected result size
		final long[] sizes = new long[]{SUBJECTS.length, PREDICATES.length, NUM_OF_OBJECTS};
		long expectedResultSize = 1;
		for (int i=0; i<3; i++)
			if (FILTER_ARRAY[i].charAt(0) == VAR_INDICATOR)
				expectedResultSize*=sizes[i];
		System.out.println("ExpectedResultSize="+expectedResultSize);

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
}