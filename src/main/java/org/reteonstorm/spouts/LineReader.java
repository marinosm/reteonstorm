package org.reteonstorm.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class LineReader extends BaseRichSpout {
	private static final long serialVersionUID = -2645103149723712238L;
	
	private String inputFile;
	private SpoutOutputCollector collector;
	private FileReader fileReader;
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
		BufferedReader reader = new BufferedReader(fileReader);
		try{
			//Read all lines
			while((str = reader.readLine()) != null){
				if ((str = str.trim()).length() > 0){
//					System.out.println("marinos::spout-emit::"+str+"@"+new Timestamp(System.currentTimeMillis()));
					this.collector.emit(new Values(str),str);
				}
			}
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple",e);
		}finally{
			completed = true;
		}
	}

	/**
	 * We will create the file and get the collector object
	 */
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get(inputFile).toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
		}
		this.collector = collector;
	}

	/**
	 * Declare the output field "word"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}
