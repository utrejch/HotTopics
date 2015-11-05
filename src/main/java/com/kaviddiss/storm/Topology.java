package com.kaviddiss.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * Topology class that sets up the Storm topology for this sample.
 * Please note that Twitter credentials have to be provided as VM args, otherwise you'll get an Unauthorized error.
 * @link http://twitter4j.org/en/configuration.html#systempropertyconfiguration
 */
public class Topology {


	static final String TOPOLOGY_NAME = "storm-twitter-word-count";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);
		//config.setDebug(true);
		String redisHost = "127.0.0.1";
		int redisPort = 6379;
		int redisDb = 0;
		TopologyBuilder b = new TopologyBuilder();
		//b.setSpout("RssSpout", new RssSpout());
		//b.setSpout("TwitterSampleSpout", new TwitterSampleSpout());
		b.setSpout("FacebookCommentsSpout", new FacebookCommentsSpout());
		//b.setSpout("FacebookTelcoSpout", new FacebookTelcoSpout());
        //b.setSpout("TwitterTrackSpout", new TwitterTrackSpout());
        //b.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("FacebookTelcoSpout");
		b.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("FacebookCommentsSpout");
		//b.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("RssSpout");
        b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).fieldsGrouping("WordSplitterBolt", new Fields("lang", "word"));
		//b.setBolt("IgnoreWordsBolt", new IgnoreWordsBolt()).fieldsGrouping("WordSplitterBolt", new Fields("word"));
        b.setBolt("WordCounterBolt", new WordCounterBolt(10, 5 * 60, 50)).fieldsGrouping("IgnoreWordsBolt",
				new Fields("word"));
		b.setBolt("WordMovingAverage", new WordMovingAverage()).globalGrouping("WordCounterBolt");
		//b.setBolt("RecordMovingAverage", new RecordMovingAverage()).globalGrouping("WordCounterBolt");
		b.setBolt("redis", new RedisIncrementBolt("Topology", redisHost, redisPort, redisDb)).
		shuffleGrouping("WordMovingAverage");
        //b.setBolt("redis", new RedisAggBolt("clients", redisHost, redisPort, redisDb)).
                //shuffleGrouping("RecordMovingAverage");
		//sentiment
		/*
		b.setBolt("sentiment", new SentimentBolt()).shuffleGrouping("FacebookTelcoSpout");
		b.setBolt("redisSentiment", new RedisSentimentBolt("Sentiment", redisHost, redisPort, redisDb)).
				shuffleGrouping("sentiment");

		*/
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}

}
