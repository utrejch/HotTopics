package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Receives tweets and emits its words over a certain length.
 */
public class WordSplitterBolt extends BaseRichBolt {
    private final int minWordLength;
    private int twitterCount;
    private OutputCollector collector;
    private static final Logger logger = LoggerFactory.getLogger(WordSplitterBolt.class);
    BufferedWriter writer;

    public WordSplitterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        twitterCount++;
        this.collector = collector;


    }

    @Override
    public void execute(Tuple input) {
        twitterCount++;

        String text = (String) input.getValueByField("tweet");
        //Status tweet = (Status) input.getValueByField("tweet");
        String lang =  "en"; //tweet.getUser().getLang();
        //String text = tweet.getText().replaceAll("\\p{Punct}", " ").toLowerCase();
        //String text = (String) input.getValueByField("title");
        String[] words = text.split(" ");

        //logger.info(tweet.getText());
        for (String word : words) {
            if (word.length() >= minWordLength) {
                collector.emit(new Values(lang, word));

            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}
