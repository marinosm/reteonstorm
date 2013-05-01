package org.reteonstorm.bolts;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.reteonstorm.Toolbox;
import org.reteonstorm.TopologyMain;

/**
 * @author Marinos Mavrommatis
 */
public class UniversalFilterMultipleStreams extends BaseBasicBolt {
	private static final long serialVersionUID = -6343942346452143072L;

	private final String[][] filters;
	private final Map<String,List<String>> fieldsGroupingVarsPerStream;


	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for (int i=0; i<filters.length; i++)
			declarer.declareStream(TopologyMain.FILTER_STREAM_PREFIX+i, new Fields("bindings", TopologyMain.FIELDS_GROUPING_VAR));
	}

	public UniversalFilterMultipleStreams(String[][] filters, Map<String,List<String>> fieldsGroupingVarsPerStream) {

		for (String[] filter : filters)
			if (filter.length != 3)
				throw new IllegalArgumentException(
						"subjPredObj must be a String array of size 3. Given array of length "
								+filter.length);

		this.filters = filters;
		this.fieldsGroupingVarsPerStream = fieldsGroupingVarsPerStream;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String[] triple = StringUtils.split(line,TopologyMain.DELIM);
		if (triple.length != 3)
			throw new RuntimeException("Line is not a triple: "+input);

		Map<Integer,Map<String,String>> allBindings = new HashMap<Integer,Map<String,String>>(filters.length);
		filter: for (int i=0; i<filters.length; i++){
			Map<String, String> bindings = new TreeMap<String, String>();
			for (int j=0; j<3; j++)
				if (filters[i][j].startsWith(TopologyMain.VAR_INDICATOR)){
					if (bindings.containsKey(filters[i][j])){
						if (!bindings.get(filters[i][j]).equals(triple[j])){
							continue filter;
						}
					}else{
						bindings.put(filters[i][j], triple[j]);
					}
				}else{
					if (!triple[j].equals(filters[i][j])){
						continue filter;
					}
				}
//			System.out.println("UniversalFilterEmitEach: Emitting for filter "+i);
			allBindings.put(i,bindings);
		}
		for (Entry<Integer, Map<String,String>> numberedBindings : allBindings.entrySet()){
			Set<String> vars = new TreeSet<String>(numberedBindings.getValue().keySet());
			String streamId = TopologyMain.FILTER_STREAM_PREFIX+numberedBindings.getKey();
			vars.retainAll(fieldsGroupingVarsPerStream.get(new GlobalStreamId(TopologyMain.FILTER_PREFIX, streamId).toString()));
			Collection<String> values = new ArrayList<String>();
			for (String var : vars)
				values.add(numberedBindings.getValue().get(var));
			collector.emit(streamId, new Values(numberedBindings.getValue(), Toolbox.myToString(values)));
		}
	}

	public void cleanup() {}

}
