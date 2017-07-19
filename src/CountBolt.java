import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class CountBolt implements IRichBolt {

	Map<String, Integer> map = new HashMap<>();
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
	}
	
	@Override
	public void execute(Tuple input) {
		String word = (String) input.getValueByField("palavra");
		
		if (map.containsKey(word)) {
			map.replace(word, map.get(word)+1);
		}
		else {
			map.put(word, 1);
		}
		
		System.out.println(map.toString());
		
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
