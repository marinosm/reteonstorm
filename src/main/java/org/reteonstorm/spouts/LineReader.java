package org.reteonstorm.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class LineReader extends BaseRichSpout {
	private static final long serialVersionUID = -2645103149723712238L;

	private static final Logger logger = Logger.getLogger(LineReader.class);
	private int countCalls;
	private int countEmits;

	private String inputFile;
	private SpoutOutputCollector collector;
	private BufferedReader heldReader;
	private boolean completed = false;
	
	public LineReader(String inputFile){
		this.inputFile = inputFile;
	}
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}
	public void close() {}
	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
	}
	
	

	/**
	 * Trim each file line and emit if non-empty
	 */
	public void nextTuple() {
		// Log when the end of file is reached, every 1000 calls
		// Log when every 1000th triple is emitted
		countCalls++;
		if (countCalls % 1000 == 0){
			logger.info("LineReader.nextTuple():countCalls = "+countCalls);
		}
		/**
		 * nextTuple is called forever, so if we have read the file
		 * we will wait and then return
		 */
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
		}
		String str;
		//Open the reader
		try{
			//Read all lines
			if((str = heldReader.readLine()) != null){
				if ((str = str.trim()).length() > 0){
					System.out.println("LineReader:line="+str);
//					System.out.println("marinos::spout-emit::"+str+"@"+new Timestamp(System.currentTimeMillis()));
					this.collector.emit(new Values(str),str);
					countEmits++;
					if (countEmits % 1000 == 0){						
						logger.info("LineReader.nextTuple():countEmits = "+countEmits);
					}
				}
			}else{
				logger.info("LineReader.nextTuple():BufferedReader returned null");
				completed = true;
			}
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple",e);
		}finally{
		}
	}

	/**
	 * We will create the file and get the collector object
	 */
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.heldReader = new BufferedReader(new FileReader(conf.get(inputFile).toString()));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
		}
		this.collector = collector;
		countCalls = 0;
		countEmits = 0;
	}

	/**
	 * Declare the output field "word"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}
