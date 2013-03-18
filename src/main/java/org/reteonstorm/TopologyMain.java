package org.reteonstorm;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import net.lag.kestrel.thrift.QueueInfo;

import org.apache.thrift7.TException;
import org.openimaj.kestrel.SimpleKestrelClient;
import org.reteonstorm.bolts.CounterTerminal;
import org.reteonstorm.bolts.KestrelTerminal;
import org.reteonstorm.bolts.Terminal;
import org.reteonstorm.bolts.TripleFilter;
import org.reteonstorm.spouts.LineReader;

import scala.actors.threadpool.Arrays;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.KestrelThriftClient;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * This is to test whether there is measurable benefit in avoiding to create multiple identical Storm Bolts
 * (for now the identical filter Bolts consume identical input)
 * 
 * TODO: investigate whether CounterTerminal is a bottleneck => increase parallelism + every x tuples received (and on cleanup), emit x_count (and later cleanup_count) to another Bolt that has parallelism=1 
 * TODO: can play with Kestrel Thrift triple-expiration (when putting triple), to also look at data windows
 * TODO: how about those zeroMQ queues, where do they get their memory from
 * TODO: use various groupings to subscribe to the shared/duplicated node
 * TODO: dynamically adding/removing queries
 * TODO: only one spout?
 * TODO: consider increasing parallelism when node sharing to avoid dead time until triple is filtered
 * TODO: understand "tasks" (can also be specified for each Bolt)
 * TODO: what if identical Bolts but different inputs
 * TODO: Check for identical Joins as well
 * TODO: Let the user input the "query": What is the effort of detecting identical nodes in the query?
 * TODO: re-take measurements on Cluster (problem with globals) (problem with timestamps when monitoring?)
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
	private static final String JOIN_NAME = "join";
	private static final String FILTER_NAME = "filter";
	private static final String FILE_SPOUT = "line-reader";
	private static final String INPUT_FILE_CONFIG = "inputFile";
	private static final String[] SUBJECTS = new String[]{"A","B","C"};
	private static final String[] PREDICATES = new String[]{"foo","bar"};
	private static final String DELIM = "_";
	private static final char VAR_INDICATOR = '?';

	/*
	 * TODO
	 */
	private static long NUM_OF_OBJECTS = 1000;
	/*
	 * how long (in seconds) between submitting and killing the topology
	 */
	private static int TIME_TO_LIVE = 45;
	/*
	 * which file to read the triples from
	 */
	private static String INPUT_FILE_NAME = "/Users/user/storm/reteonstorm/resources/abcd";
	/*
	 * Whether to avoid creating identical filter Bolts (Named after "node sharing" in the Rete algorithm)
	 * If this is true, only a single filter Bolt is created that emits to all Terminal Bolts
	 * If false, multiple identical filter Bolts are created each emitting to a different Terminal Bolt
	 */
	private static boolean SHARING = true;
	/*
	 * TODO
	 */
	private static String FILTER = "?a_foo_?b";
	/*
	 * Specifies how many Terminal Bolts should be created.
	 * Also in case SHARING is false, this also specifies how many identical Filter nodes should be created
	 */
	private static int NUM_OF_IDENTICAL_NODES = 5;
	/*
	 * The following variables specify the parallelism_hint (initial number of executors) for each type of Bolt
	 */
	private static int SPOUT_PARALLELISM = 5;
	private static int FILTER_PARALLELISM = 5;
	//	private static int JOIN_PARALLELISM = 1;// = 3;
	private static int TERMINAL_PARALLELISM = 1;

	private static String TOPOLOGY_NAME = "Node-Sharing-Evaluation";

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException, TException {

		//Arguments
		for (int i=0; i<args.length; i+=2){
			if (args[i].equals("-f"))
				INPUT_FILE_NAME = args[i+1];
			else if (args[i].equals("-ns"))
				SHARING = Boolean.parseBoolean(args[i+1]);
			else if (args[i].equals("-n"))
				NUM_OF_IDENTICAL_NODES = Integer.parseInt(args[i+1]);
			else if (args[i].equals("-F"))
				FILTER = args[i+1];
			else if (args[i].equals("-sp")) //TODO: each spout has its own line-counter (solve by popping of a Queue, but how about reading off the )
				SPOUT_PARALLELISM = Integer.parseInt(args[i+1]);
			else if (args[i].equals("-fp"))
				FILTER_PARALLELISM = Integer.parseInt(args[i+1]);
//			else if (args[i].equals("-tp")) //FIXME: probably fails to count when this is greater than one (not for KestrelTerminal)
//				TERMINAL_PARALLELISM = Integer.parseInt(args[i+1]);
			//			if (args[i].equals("-jp")) TODO: joins not implemented yet
			//				JOIN_PARALLELISM = Integer.parseInt(args[i+1]);
			else if (args[i].equals("-i"))
				NUM_OF_OBJECTS = Integer.parseInt(args[i+1]);
			else if (args[i].equals("-ttl"))
				TIME_TO_LIVE = Integer.parseInt(args[i+1]);
		}

		//init
		TopologyBuilder sharingBuilder = new TopologyBuilder();
		String[] FILTER_ARRAY = FILTER.split(DELIM);
		if (FILTER_ARRAY.length != 3)
			throw new IllegalArgumentException("FILTER_ARRAY must be a triple of 3 strings separated by underscores");

		//clean and fill up inputQ
		//		SimpleKestrelClient client = new SimpleKestrelClient(KESTREL_IP, KESTREL_MEMCACHED_PORT);
		//		client.delete(INPUT_QUEUE);
		KestrelThriftClient client = new KestrelThriftClient(KESTREL_IP, KESTREL_THRIFT_PORT);
		client.delete_queue(INPUT_QUEUE);
		for (String subj: SUBJECTS){
			for (String pred: PREDICATES){
				for (int obj=0; obj<NUM_OF_OBJECTS; obj++){
					String triple = subj +DELIM+ pred +DELIM+ obj;

					client.put(INPUT_QUEUE, triple, 0);
					//					Thread.sleep(6);
					//					client.set(INPUT_QUEUE, triple);
				}
			}
		}
		//				client.close(); TODO: do I need to close now?
		System.out.println("Input generated. "+INPUT_QUEUE+" contains "+client.peek(INPUT_QUEUE).get_items()+" items");
		//Spout
		KestrelThriftSpout kestrelSpout = new KestrelThriftSpout(KESTREL_IP, KESTREL_THRIFT_PORT, INPUT_QUEUE, new StringScheme());
		//		sharingBuilder.setSpout(FILE_SPOUT,new LineReader(INPUT_FILE_CONFIG), SPOUT_PARALLELISM);
		sharingBuilder.setSpout(KESTREL_SPOUT, kestrelSpout, SPOUT_PARALLELISM);

		//Single or multiple identical Bolts (depending on whether node-sharing is enabled)
		Object filters;
		if (SHARING) {
			filters = FILTER_NAME;

			sharingBuilder.setBolt((String) filters, new TripleFilter(FILTER_ARRAY, DELIM, VAR_INDICATOR), FILTER_PARALLELISM)
			/* 
			 * instances of the (single) filter should share the workload evenly 
			 */
			.shuffleGrouping(KESTREL_SPOUT);
			//				.shuffleGrouping(FILE_SPOUT);
		} else {
			filters = new ArrayList<String>(NUM_OF_IDENTICAL_NODES);

			for (int i = 0; i < NUM_OF_IDENTICAL_NODES; i++){
				String currentName = FILTER_NAME+"-"+i;

				sharingBuilder.setBolt(currentName, new TripleFilter(FILTER_ARRAY, DELIM, VAR_INDICATOR), FILTER_PARALLELISM)
				/*
				 * Multiple Bolts doing exactly the same job
				 * If the spout reads from a file then input to all bolts is exactly the same (redundancy)
				 * 
				 * Each of them has a number of instances that share its workload evenly
				 */
				.shuffleGrouping(KESTREL_SPOUT);
				//					.shuffleGrouping(FILE_SPOUT);
				((List<String>) filters).add(currentName);
			}
		}

		/* 
		 * create a number of Terminals. 
		 * if SHARING, all subscribe to the only Filter Bolt created, 
		 * otherwise each subscribes to a different Filter Bolt 
		 */
		String currentFilter = SHARING? (String)filters : null;
		for (int i = 0; i < NUM_OF_IDENTICAL_NODES; i++){
			if (!SHARING)
				currentFilter = ((List<String>)filters).remove(0);
			sharingBuilder.setBolt(TERMINAL_NAME+"-"+i, new CounterTerminal(i), TERMINAL_PARALLELISM)
			//			sharingBuilder.setBolt(TERMINAL_NAME+"-"+i, new KestrelTerminal(OUTPUT_QUEUE+i), TERMINAL_PARALLELISM)
			//			sharingBuilder.setBolt(TERMINAL_NAME+"-"+i, new Terminal(), TERMINAL_PARALLELISM)
			.shuffleGrouping(currentFilter); //TODO: should also be tested with fieldsGrouping (not trivial!) and allGrouping
			//				.fieldsGrouping(currentFilter, new Fields("value=a|b")); 
		}

		//Configuration
		Config conf = new Config();
		conf.put(INPUT_FILE_CONFIG, INPUT_FILE_NAME);
		conf.setDebug(true);
		//Topology run
		//		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();

		cluster.submitTopology(TOPOLOGY_NAME, conf, sharingBuilder.createTopology());

		Utils.sleep(TIME_TO_LIVE*1000);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();

		//determine expected result size
		final long[] sizes = new long[]{SUBJECTS.length, PREDICATES.length, NUM_OF_OBJECTS};
		long expectedResultSize = 1;
		for (int i=0; i<3; i++){
			if (FILTER_ARRAY[i].charAt(0) == VAR_INDICATOR){
				expectedResultSize*=sizes[i];
			}
		}
		System.out.println("ExpectedResultSize="+expectedResultSize); //TODO logger.INFO instead
//
//		//check output size for each OutputQ (ie for each terminal node)
//		//		client = new SimpleKestrelClient(KESTREL_IP, KESTREL_MEMCACHED_PORT);
//		boolean resultsOK = true;
//		//		int[] counters = new int[NUM_OF_IDENTICAL_NODES];
//		for (int i = 0; i < NUM_OF_IDENTICAL_NODES; i++){
//			//			counters[i] = 0;
//			//		while (client.get(OUTPUT_QUEUE+i) != null){
//			//			counters[i]++;
//			//		}
//			//			if (counters[i] != expectedResultSize){
//			//				System.out.println(OUTPUT_QUEUE+i+": tuples found = "+counters[i]); 
//			//				resultsOK = false;
//			//			}
//			//			client.get(OUTPUT_QUEUE+i, expectedResultSize, timeout_msec, auto_abort_msec)
//			//TODO: documentation https://github.com/robey/kestrel/blob/master/src/main/thrift/kestrel.thrift
//
//			long items = client.peek(OUTPUT_QUEUE+i).get_items();
//			System.out.println(OUTPUT_QUEUE+i+" contains "+items+" items");
//			if (items != expectedResultSize)
//				resultsOK=false;
//		}
//		System.out.println("Results_OK="+resultsOK);
		client.close();
		//		System.exit(0);
	}
}