package org.reteonstorm;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift7.TException;
import org.reteonstorm.bolts.KestrelTerminal;
import org.reteonstorm.bolts.TripleFilter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.KestrelThriftSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * This is to test whether there is measurable benefit in avoiding to create multiple identical Storm Bolts
 * (for now the identical filter Bolts consume identical input)
 * 
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
	private static final int KESTREL_PORT = 22133;
	private static final String KESTREL_IP = "localhost";
	private static final String TERMINAL_NAME = "terminal";
	private static final String JOIN_NAME = "join";
	private static final String FILTER_NAME = "filter";
	private static final String FILE_SPOUT = "mixed-reader";
	private static final String INPUT_FILE_CONFIG = "inputFile";
	private static final String[] FILTER = new String[]{"?a","pred=foo","obj=0"};
//	private static final String[] FILTER = new String[]{"?a","pred=foo","?b"};
	
	
	/*
	 * how long (in seconds) between submitting and killing the topology
	 */
	private static int TIME_TO_LIVE = 20;
	/*
	 * which file to read the triples from
	 */
	private static String INPUT_FILE_NAME = "/Users/user/storm/reteonstorm/resources/abcd.txt";
	/*
	 * Whether to avoid creating identical filter Bolts (Named after "node sharing" in the Rete algorithm)
	 * If this is true, only a single filter Bolt is created that emits to all Terminal Bolts
	 * If false, multiple identical filter Bolts are created each emitting to a different Terminal Bolt
	 */
	private static boolean SHARING = true;
	/*
	 * Specifies how many Terminal Bolts should be created.
	 * Also in case SHARING is false, this also specifies how many identical Filter nodes should be created
	 */
	private static int NUM_OF_IDENTICAL_NODES = 2;
	/*
	 * The following variables specify the parallelism_hint (initial number of executors) for each type of Bolt
	 */
	private static int TERMINAL_PARALLELISM = 1;
	private static int SPOUT_PARALLELISM = 1;
	private static int FILTER_PARALLELISM = 1;
//	private static int JOIN_PARALLELISM = 1;// = 3;
	
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
//			if (args[i].equals("-tp")) FIXME: probably fails to count when this is greater than one
//				TERMINAL_PARALLELISM = Integer.parseInt(args[i+1]);
			else if (args[i].equals("-sp")) //TODO: each spout has its own line-counter (solve by popping of a Queue, but how about reading off the )
				SPOUT_PARALLELISM = Integer.parseInt(args[i+1]);
			else if (args[i].equals("-fp"))
				FILTER_PARALLELISM = Integer.parseInt(args[i+1]);
//			if (args[i].equals("-jp")) TODO: joins not implemented yet
//				JOIN_PARALLELISM = Integer.parseInt(args[i+1]);
			else if (args[i].equals("-ttl"))
				TIME_TO_LIVE = Integer.parseInt(args[i+1]);
		}
		
		//init
		TopologyBuilder sharingBuilder = new TopologyBuilder();
		
		//make this a Unit-Test
//		SimpleKestrelClient client = new SimpleKestrelClient(KESTREL_IP, KESTREL_PORT);
//		client.set(INPUT_QUEUE, "test");
//		String test = client.get(INPUT_QUEUE);
//		System.out.println("TopologyMain:result:"+test);
//		client.close();
//		System.exit(0);
		
		//Spout
		KestrelThriftSpout kestrelSpout = new KestrelThriftSpout(KESTREL_IP, KESTREL_PORT, INPUT_QUEUE, new StringScheme());
//		sharingBuilder.setSpout(FILE_SPOUT,new LineReader(INPUT_FILE_CONFIG), SPOUT_PARALLELISM);
		sharingBuilder.setSpout(KESTREL_SPOUT, kestrelSpout, SPOUT_PARALLELISM);
		
		//Single or multiple identical Bolts (depending on whether node-sharing is enabled)
		Object filters;
		if (SHARING) {
			filters = FILTER_NAME;
			
			sharingBuilder.setBolt((String) filters, new TripleFilter(0, FILTER_NAME, FILTER), FILTER_PARALLELISM)
				/* 
				 * instances of the (single) filter should share the workload evenly 
				 */
				.shuffleGrouping(KESTREL_SPOUT);
		} else {
			filters = new ArrayList<String>(NUM_OF_IDENTICAL_NODES);
			
			for (int i = 0; i < NUM_OF_IDENTICAL_NODES; i++){
				String currentFilter = FILTER_NAME+"-"+i;
				
				sharingBuilder.setBolt(currentFilter, new TripleFilter(i, FILTER_NAME+"-"+i, FILTER), FILTER_PARALLELISM)
					/*
					 * Multiple Bolts doing exactly the same job
					 * If the spout reads from a file then input to all bolts is exactly the same (redundancy)
					 * 
					 * Each of them has a number of instances that share its workload evenly
					 */
					.shuffleGrouping(KESTREL_SPOUT);
				((List<String>) filters).add(currentFilter);
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
//			sharingBuilder.setBolt(JOIN_NAME+"-"+i, new DummyJoin(), JOIN_PARALLELISM)
//				.shuffleGrouping(currentFilter);
			sharingBuilder.setBolt(TERMINAL_NAME+"-"+i, new KestrelTerminal(), TERMINAL_PARALLELISM)
//				.fieldsGrouping(currentFilter, new Fields("value=a|b")); 
				.shuffleGrouping(currentFilter); //TODO: should also be tested with fieldsGrouping (not trivial!) and allGrouping
		}
		
        //Configuration
		Config conf = new Config();
		conf.put(INPUT_FILE_CONFIG, INPUT_FILE_NAME);
		conf.setDebug(true);
        //Topology run
//		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology(TOPOLOGY_NAME, conf, sharingBuilder.createTopology());

//		System.out.println("marinos:topology-submitted@"+new Timestamp(System.currentTimeMillis()));
		
		Utils.sleep(TIME_TO_LIVE*1000);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}
}
