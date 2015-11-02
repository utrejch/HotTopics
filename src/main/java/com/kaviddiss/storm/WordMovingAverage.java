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

import java.io.*;
import java.sql.Timestamp;
import java.util.*;

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
    private LinkedList<String> zeros;
    private Long lzero;
    private Writer writer;
    public WordMovingAverage(){


    }
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        macounter = new HashMap <String, CircularFifoQueue<Long>> ();
        try {
            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("counts.txt"), "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
        this.collector = collector;
        zeros = new LinkedList<String>();
        lzero = new Long(0);
    }

    @Override
    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        Long count = (Long) input.getValueByField("count");
        //logger.info(word + " " + count);
        zeros.add(word);
        if(macounter.containsKey(word)){
            macounter.get(word).add(count);
        }
        else {
            CircularFifoQueue<Long> cfq = new CircularFifoQueue<Long>(CFQLIMIT);
            cfq.add(count);
            macounter.put(word, cfq);
        }

        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;

        if (logPeriodSec > 30) {
            for( Map.Entry<String,CircularFifoQueue<Long>> entry : macounter.entrySet()) {
                String kw= entry.getKey();

                if (!zeros.contains(kw)) {

                    macounter.get(kw).add(lzero);
                }
            }

            lastLogTime = now;
            publishMA();
        }
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

        for (Map.Entry<Double, String> ent : top1.entrySet()) {
            double count = ent.getKey();
            String word = ent.getValue();
            logger.info(new StringBuilder("#top:\t").append(word).append(':').append(count).toString());
            collector.emit(new Values(word, count));
        }
        zeros.clear();

    }
    public void publishAgg(){
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
        if (logPeriodSec > 600) {

            lastLogTime = now;
            StringBuilder sb = new StringBuilder();
            java.util.Date date= new java.util.Date();
            Timestamp tmps = new Timestamp(date.getTime());
            for (Map.Entry<Double, String> ent : top1.entrySet()) {
                double count = ent.getKey();
                String word = ent.getValue();
                word = word.replace(";", "");
                sb.append(word).append(";").append(count).append("|");
                logger.info(new StringBuilder("top - ").append(word).append('>').append(count).toString());
                try {
                    writer.write(new StringBuilder().append(tmps.toString()).append("\t")
                            .append(word).append("\t").append(count).toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            collector.emit(new Values(sb.toString()));
        }
    }
}
