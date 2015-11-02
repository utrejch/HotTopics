/**
 * Taken from the storm-starter project on GitHub
 * https://github.com/nathanmarz/storm-starter/ 
 */
package com.kaviddiss.storm;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.base.Preconditions;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import com.sun.syndication.io.XmlReader;
import twitter4j.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.Thread.sleep;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 * @author davidk
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class RssSpout extends BaseRichSpout{

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<String> queue;
    SyndFeedInput input;
    HashMap<URL, Date> urlCache;

    public static HashMap<URL, Date> createRssCache(String path){
        HashMap<URL, Date> urlCache = new HashMap<URL, Date>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(path));
            String line;
            while ((line = br.readLine()) != null) {

                urlCache.put(new URL(line), new Date(0));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return urlCache;
    }

    public void fillQueue(){
        Iterator urlIter = urlCache.entrySet().iterator();
        while (urlIter.hasNext()) {
            Map.Entry pair = (Map.Entry) urlIter.next();
            Date pivotDate = (Date) pair.getValue();
            System.out.print(pair.getKey().toString() + " "  );
            System.out.println(pivotDate.toString());
            SyndFeed feed = null;
            try {
                feed = input.build(new XmlReader((URL) pair.getKey()));
            } catch (FeedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            List list = feed.getEntries();
            for (final ListIterator iter = list.listIterator(list.size());
                 iter.hasPrevious(); ) {

                final SyndEntry entry = (SyndEntry) iter.previous();
                String title = entry.getTitle();
                //String uri = entry.getUri();
                //Date date = entry.getPublishedDate();
                if (entry.getPublishedDate() != null && pivotDate.before(entry.getPublishedDate())) {

                    pivotDate = entry.getPublishedDate();
                    urlCache.put((URL) pair.getKey(), pivotDate);
                    //System.out.println(pivotDate.toString());
                    //System.out.println(feed.getTitle().toString() + ": " + entry.getPublishedDate().toString() + " " + title);
                    queue.offer(title);
                }
                //feed.clear();
                // size = 1;
            }


        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("title"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
       collector = spoutOutputCollector;
       urlCache = createRssCache("urls.txt");
       input = new SyndFeedInput();
        queue = new LinkedBlockingQueue<String>();
    }

    @Override
    public void nextTuple() {
        try {
            sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String title = queue.poll();
        if(title != null){
            collector.emit(new Values(title));
        }
        else {
            fillQueue();
        }

            //feed.clear();
            // size = 1;

    }
}
