package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by werni on 14/09/15.
 */



public class WordMovingAverage extends BaseRichBolt {

    /** Number of seconds before the top list will be logged to stdout. */
    private static final Logger logger = LoggerFactory.getLogger(WordMovingAverage.class);
    private int movingAverageWindow = 1000;
    private long lastLogTime;
    private long lastClearTime;
    private int CFQLIMIT = 10;
    private int madenom;
    private Map<String, CircularFifoQueue<Long>> macounter;
    private OutputCollector collector;

    public WordMovingAverage(){

    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        macounter = new HashMap <String, CircularFifoQueue<Long>> ();

        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
        this.collector = collector;

    }

    @Override
    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        Long count = (Long) input.getValueByField("count");
        //logger.info(word + " " + count);
        if(macounter.containsKey(word)){
            macounter.get(word).add(count);
        }
        else {
            CircularFifoQueue<Long> cfq = new CircularFifoQueue<Long>(CFQLIMIT);
            cfq.add(count);
            macounter.put(word, cfq);
        }
        publishMA();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));

    }

    public void publishMA(){
        SortedMap<Double, String> top1 = new TreeMap<Double, String>();
        for( Map.Entry<String,CircularFifoQueue<Long>> entry : macounter.entrySet()){
            String word = entry.getKey();
            int masize = entry.getValue().size();
            if(madenom < masize){
                madenom = masize;
            }
            double sum =  0.0;
            for(Long val : entry.getValue()){
                sum += val;
            }
            double ma = sum / madenom;
            top1.put(ma, word);
            if (top1.size() > 30) {
                top1.remove(top1.firstKey());
            }
            //if( ma > 5) logger.info(new StringBuilder("ma - ").append(entry.getKey()).append(" ").append(ma).toString());
        }
        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > 30) {

            lastLogTime = now;

            for (Map.Entry<Double, String> ent : top1.entrySet()) {
                String word = ent.getValue();
                String count = ent.getValue();
                collector.emit(new Values(ent.getValue(), ent.getKey()));
                logger.info(new StringBuilder("top - ").append(ent.getValue()).append('>').append(ent.getKey()).toString());
            }
        }
    }
}
