package com.kaviddiss.storm;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Increment key in sorted set
 */
public class RedisAggBolt extends BaseRichBolt {
    private static final long serialVersionUID = -2819069215379325159L;
    private final String key;
    private final String redisHost;
    private final int redisPort;
    private final int redisDb;
    private Jedis redis;
    private OutputCollector collector;

    /**
     * @param key
     * @param redisHost
     * @param redisPort
     * @param redisDb
     */
    public RedisAggBolt(final String key, final String redisHost, final int redisPort, final int redisDb) {
        super();
        this.key = key;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisDb = redisDb;
    }

    @Override
    public void prepare(final Map stormConf, final TopologyContext context,
                        final OutputCollector collector) {
        redis = new Jedis(redisHost, redisPort);
        //redis.select(redisDb);
        redis.connect();
        this.collector = collector;
    }

    @Override
    public void execute(final Tuple input) {
        String record = (String) input.getValueByField("record");
        //redis.set(word, Double.toString(count)) ;
        StringBuilder sb = new StringBuilder();
        redis.publish("Topology", sb.append("date;").append(getTimestamp()).append("|").append(record).toString());
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
    }

    private String getTimestamp(){
        Long d = System.currentTimeMillis();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formattedDate = dateFormat.format(d);
        return formattedDate;
    }

}
