package org.reteonstorm.bolts;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class Terminal extends BaseBasicBolt {
	private static final long serialVersionUID = -7711580647434264410L;
	
	Integer id;
	String name;
	List<TimedBindings> received;

	@Override
	public void cleanup() {
		System.out.println("<result>");
		for (TimedBindings tb : received)
			System.out.println(tb.myToStirng());
		System.out.println("</result>");
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		this.received = new ArrayList<TimedBindings>();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		@SuppressWarnings("unchecked")
		TimedBindings tuple = new TimedBindings(
				(Map<String,String>)input.getValue(0), 
				new Timestamp(System.currentTimeMillis()) //FIXME: what happens when deployed to >1 machines
				); 
		received.add(tuple);
		
//		System.out.println("marinos::terminal-received-tuple::"+tuple.myToStirng()+" @Terminal:"+name+"(id="+id+")");
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
	
	private class TimedBindings implements Serializable {
		private static final long serialVersionUID = -4210844769466216206L;
		
		private final Map<String,String> tuple;
		private final Timestamp timestamp;
		public TimedBindings(Map<String,String> tuple, Timestamp timestamp) { 
			this.tuple = tuple;
			this.timestamp = timestamp; 
		}
		public String myToStirng(){
			return "bindings"+tuple+" timestamp="+timestamp;
		}
//		public String getTuple() { return tuple; }
//		public Timestamp getTs() { return timestamp; }
	}
}
