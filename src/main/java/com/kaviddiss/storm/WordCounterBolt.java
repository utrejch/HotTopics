package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Keeps stats on word count, calculates and logs top words every X second to stdout and top list every Y seconds,
 * @author davidk
 */
public class WordCounterBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);
    /** Number of seconds before the top list will be logged to stdout. */
    private final long logIntervalSec;
    /** Number of seconds before the top list will be cleared. */
    private final long clearIntervalSec;
    /** Number of top words to store in stats. */
    private final int topListSize;

    private Map<String, Long> counter;
    private long lastLogTime;
    private long lastClearTime;
    private OutputCollector collector;
    private HashSet<String> stoplist;

    public WordCounterBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
        this.logIntervalSec = logIntervalSec;
        this.clearIntervalSec = clearIntervalSec;
        this.topListSize = topListSize;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        counter = new HashMap<String, Long>();
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
        this.collector = collector;

        String s = null;
        try {
            s = IOUtils.toString(getClass().getResourceAsStream("/resources/kw2ten.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        stoplist = new HashSet<String> (Arrays.asList(s.split("\n")));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }

    @Override
    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        String lang = (String) input.getValueByField("lang");
        //String query = new StringBuilder(lang).append(".").append(word).toString();
        String query = word;
        Long count = counter.get(query);
        count = count == null ? 1L : count + 1;
        if(!stoplist.contains(query)) counter.put(query, count);
        //logger.info(query);
        //logger.info(new StringBuilder(word).append('>').append(count).toString());

        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > 500) {
        //if (logPeriodSec > logIntervalSec) {
            logger.info("Word count: " + counter.size());

            publishTopList();
            lastLogTime = now;
        }
    }

    private void publishTopList() {
        // calculate top list:
        SortedMap<Long, String> top = new TreeMap<Long, String>();
        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            long count = entry.getValue();
            String word = entry.getKey();
            logger.debug(new StringBuilder(word).append(" ").append(count).toString());
            top.put(count, word);
            if (top.size() > topListSize) {
                top.remove(top.firstKey());
            }
        }

        // Output top list:

       /* for (Map.Entry<Long, String> entry : top.entrySet()) {
            logger.info(new StringBuilder("top - ").append(entry.getValue()).append('>').append(entry.getKey()).toString());
        }*/

        // Clear top list
        long now = System.currentTimeMillis();
        //if (now - lastClearTime > clearIntervalSec * 1000) {
            counter.clear();
            lastClearTime = now;
            for (Map.Entry<Long, String> entry : top.entrySet()) {
                if(entry.getKey() > 0)
                    //logger.info(new StringBuilder("wc - ").append(entry.getValue()).append('>').append(entry.getKey()).toString());
                    collector.emit( new Values(entry.getValue(), entry.getKey()));
          //  }
        }
    }
}
