/**
 * Taken from the storm-starter project on GitHub
 * https://github.com/nathanmarz/storm-starter/ 
 */
package com.kaviddiss.storm;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.base.Preconditions;
import twitter4j.*;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 * @author davidk
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterTrackSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;
    private String[] tracks;

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        this.collector = collector;
        tracks = new String[] {"TDC", "Telia Denmark",  "YouSee", "BiBoB", "Telmore", "Fullrate", "Callme"};

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
            }

            @Override
            public void onException(Exception e) {
            }
        };

        TwitterStreamFactory factory = new TwitterStreamFactory();
        twitterStream = factory.getInstance();
        twitterStream.addListener(listener);

        //twitterStream.filter(new FilterQuery(0, new long[0],new String[]{"telenor"}));
        FilterQuery ft = new FilterQuery();
        double[][] multi = new double[2][];
        multi[0] = new double[]{7.738874,54.963716 };
        multi[1] = new double[] {11.607114, 57.545517};
        //ft.locations(multi);
        ft.language("dn");
        twitterStream.filter(ft);
    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if (ret == null) {
            Utils.sleep(5);
        } else {
            collector.emit(new Values(ret));
        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}
