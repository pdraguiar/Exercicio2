import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Main {

	public static void main(String[] args) {
		Config config = new Config();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("array-spout", new ArraySpout());
		//builder.setBolt("splitter-bolt", new SplitterBolt()).shuffleGrouping("array-spout");
		//builder.setBolt("word-count", new CountBolt()).shuffleGrouping("splitter-bolt");
		builder.setBolt("view-taxi-bolt", new ViewTaxiBolt()).shuffleGrouping("array-spout");
		
		LocalCluster local = new LocalCluster();
		local.submitTopology("UPE cecda", config, builder.createTopology());

	}

}
