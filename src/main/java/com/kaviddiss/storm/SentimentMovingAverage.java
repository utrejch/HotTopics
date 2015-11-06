package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.collections4.queue.CircularFifoQueue;

import java.util.*;

import static java.lang.Thread.sleep;
/**
 * Created by werni on 03/11/15.
 */
public class SentimentMovingAverage extends BaseRichBolt {
    private OutputCollector collector;
    private Map<Integer, CircularFifoQueue<Double>> macounter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        macounter = new TreeMap<Integer, CircularFifoQueue<Double>>();
        this.collector = collector;

    }

    @Override
    public void execute(final Tuple tuple) {
        String agg = (String) tuple.getValueByField("agg");
        String[] parts = agg.split("\\|");

        for (int i = 0; i < parts.length; i++) {
            if (macounter.containsKey(i)) {
                macounter.get(i).add(Double.parseDouble(parts[i]));
            } else {
                CircularFifoQueue<Double> cfq = new CircularFifoQueue<Double>(4);
                cfq.add(Double.parseDouble(parts[i]));
                macounter.put(i, cfq);
            }
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, CircularFifoQueue<Double>> entry : macounter.entrySet()) {
            int key = entry.getKey();
            int masize = entry.getValue().size();
            double sum = 0.0;
            for (Double val : entry.getValue()) {
                sum += val;
            }

            double ma = sum / masize;
            if (key < 2) {
                sb.append(ma).append("|");
            } else {
                sb.append(ma);
            }
        }

        try {
            sleep(700);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
            collector.emit(new Values(sb.toString()));

    }

    @Override
    public void declareOutputFields (OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("agg"));
    }
}
