import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ArraySpout implements IRichSpout {
	SpoutOutputCollector collector;
	ActiveMQConnectionFactory connectionFactory;
	Connection connection;
	Session session;
	Destination destination;
	MessageConsumer consumer;

	int count = 0;
	String[] data = { "Sport Campeao", "Nautico Lanterna", "UPE Caruaru Show", "POli melhor depois de Caruaru",
			"Ciencia de Dados", "Almoco Varanda", "Davi Rico com Chumbo", "Eronita e Carol Chefe ATI",
			"talita ganha 14mil", "Seu Francisco home da estatistica", "Vitor mendoca o cara da APAC",
			"Carlos Vera Diego Souza" };

	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		try {
			// Create a ConnectionFactory
			connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			// Create a Connection
			connection = connectionFactory.createConnection();
			// Create a Session
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			// Create the destination (Topic or Queue)
			destination = session.createQueue("ROTA-7659");
			// Create a MessageConsumer from the Session to the Topic or Queue
			consumer = session.createConsumer(destination);
			
			connection.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		/*if (count >= data.length) {
			count = 0;
		}*/
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//this.collector.emit(new Values(data[count++]));
		
		TextMessage receive;
		try {
			receive = (TextMessage) consumer.receive();
			this.collector.emit(new Values(receive.getText()));
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("linha"));
	}

	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
