import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ViewTaxiBolt implements IRichBolt{

	OutputCollector collector;
	
	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}


	@Override
	public void execute(Tuple input) {
		String linha = (String) input.getValueByField("linha");
		
		System.out.println(linha);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("palavra"));
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

