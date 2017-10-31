package byr.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import byr.bolt.LogFilterBolt;
import byr.bolt.stat.UserVisitPageParseBolt;
import byr.bolt.stat.WebSiteEveryDayBolt;
import byr.bolt.stat.WebSiteIdentityBolt;
import byr.common.Constants;
import byr.spout.ReadLogSpout;
import org.apache.avro.generic.GenericData;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import storm.bolt.IntermediateRankingsBolt;
import storm.bolt.RollingCountBolt;
import storm.bolt.TotalRankingsBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.List;

import static backtype.storm.Config.STORM_ZOOKEEPER_PORT;
import static backtype.storm.Config.STORM_ZOOKEEPER_SERVERS;

/**
 * website日志
 */
public class WebLogTopology {

	private static final int TOP_N = 50;

	public static void main(String[] args) throws Exception {

		String kafkaZookeeper = "slave2:2181";
		BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "test_001", "/weblog2", "id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.zkServers =  ImmutableList.of("slave2");
		kafkaConfig.zkPort = 2181;
		kafkaConfig.ignoreZkOffsets = true;
		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("file-spout", new ReadLogSpout());
		builder.setSpout("file-spout", new KafkaSpout(kafkaConfig));
		builder.setBolt("line-filter", new LogFilterBolt(), 4).shuffleGrouping("file-spout");

		//按照用户统计
//		builder.setBolt("rolling-count", new RollingCountBolt(9, 3),4).fieldsGrouping("line-filter", new Fields("userId"));
//		builder.setBolt("intermediateRanker", new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping("rolling-count",new Fields("obj"));
//		builder.setBolt("finalRanker", new TotalRankingsBolt(TOP_N)).globalGrouping("intermediateRanker");

		//按照网站每天访问量统计
		 builder.setBolt("everyday-count", new WebSiteEveryDayBolt(),1).shuffleGrouping("line-filter");

		//按照身份统计
//		 builder.setBolt("identity-count",new WebSiteIdentityBolt(),1).shuffleGrouping("line-filter");

		//每个用户访问每个页面的频率统计
//		builder.setBolt("user-page-parse",new UserVisitPageParseBolt()).fieldsGrouping("line-filter", new Fields("userId"));

		submit(args, builder);
	}
	private static void submit(String[] args, TopologyBuilder builder) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException {
		Config conf = new Config();
		conf.setNumWorkers(3);
		//conf.setDebug(true);
		conf.put(Constants.REDIS_HOST, Constants.DEFAULT_REDIS_HOST);
		conf.put(Constants.REDIS_PORT, Constants.DEFAULT_REDIS_PORT);

		if (args != null && args.length > 0) {
			if (args.length == 3) {
				conf.put(Constants.REDIS_HOST, args[1]);
				conf.put(Constants.REDIS_PORT, args[2]);
			}
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

//			List<String> zookeeperList = new ArrayList<>();
//			zookeeperList.add(Constants.DEFAULT_REDIS_HOST);
//			conf.put(STORM_ZOOKEEPER_SERVERS, zookeeperList);
//			conf.put(STORM_ZOOKEEPER_PORT, Constants.DEFAULT_REDIS_PORT);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("local-host", conf, builder.createTopology());
			Thread.sleep(60000);
			cluster.killTopology("local-host");
			cluster.shutdown();
		}
		Utils.sleep(600000);
	}
}
