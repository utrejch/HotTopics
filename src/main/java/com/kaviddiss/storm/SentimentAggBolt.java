package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import static java.lang.Thread.sleep;
/**
 * Created by werni on 03/11/15.
 */
public class SentimentAggBolt extends BaseRichBolt {
    LinkedList<Long> scores;
    private HashMap<String, Double> counter;
    private OutputCollector collector;
    long lastLogTime;
    Double all;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        scores = new LinkedList<Long>();
        counter = new HashMap<String, Double>();
        this.collector = collector;
        lastLogTime = System.currentTimeMillis();
        all = 0.0;
    }

    @Override
    public void execute(final Tuple tuple) {
        String sscore = (String) tuple.getValueByField("tweet");
        Double score = Double.parseDouble(sscore);
        String val = "neu";

        if (score <= 0) val = "neg";
        if (score >= 4) val = "pos";
        all = all + 1;
        Double count = counter.get(val);
        count = count == null ? 1.0 : count + 1.0;
        counter.put(val, count);
        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        Double neg;
        Double neu;
        Double pos;
        System.out.println(logPeriodSec);
        System.out.println(all);
        if (logPeriodSec > 30 && all > 0) {
            //if (logPeriodSec > logIntervalSec) {
            StringBuilder sb = new StringBuilder();


            neg = counter.get("neg");
            neu = counter.get("neu");
            pos = counter.get("pos");

            neg = neg == null ? 0.0 : neg;
            neu = neu == null ? 0.0 : neu ;
            pos = pos == null ? 0.0 : pos;
            sb.append(neg / all).append("|").append(neu / all)
                    .append("|").append(pos / all).toString();

            System.out.println(sb.toString());
            collector.emit(new Values(sb.toString()));
            all = 0.0;
            counter.clear();
            lastLogTime = now;
        }
        try {
            sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("agg"));
    }
}
