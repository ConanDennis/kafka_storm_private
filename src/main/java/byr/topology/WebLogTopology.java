package byr.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import byr.bolt.LogFilterBolt;
import byr.bolt.stat.UserVisitPageParseBolt;
import byr.common.Constants;
import byr.spout.ReadLogSpout;

/**
 * website日志
 */
public class WebLogTopology {

	private static final int TOP_N = 50;

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("file-spout", new ReadLogSpout());
		builder.setBolt("line-filter", new LogFilterBolt(), 3).shuffleGrouping("file-spout");
		//按照用户统计
//		builder.setBolt("rolling-count", new RollingCountBolt(9, 3),4).fieldsGrouping("line-filter", new Fields("userId"));
//		builder.setBolt("intermediateRanker", new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping("rolling-count",new Fields("obj"));
//		builder.setBolt("finalRanker", new TotalRankingsBolt(TOP_N)).globalGrouping("intermediateRanker");
		//按照网站每天访问量统计
		// builder.setBolt("everyday-count", new
		// WebSiteEveryDayBolt(),1).shuffleGrouping("line-filter");
		//按照身份统计
		// builder.setBolt("identity-count",new
		// WebSiteIdentityBolt(),1).shuffleGrouping("line-filter");
		//每个用户访问每个页面的频率统计
		builder.setBolt("user-page-parse",new UserVisitPageParseBolt()).fieldsGrouping("line-filter", new Fields("userId"));
		submit(args, builder);
	}
	private static void submit(String[] args, TopologyBuilder builder) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException {
		Config conf = new Config();
		conf.setNumWorkers(3);
		// conf.setDebug(true);
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
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("local-host", conf, builder.createTopology());
			Thread.sleep(70000);
			cluster.shutdown();
		}
		Utils.sleep(600000);
	}
}
