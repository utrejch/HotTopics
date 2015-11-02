package com.kaviddiss.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import facebook4j.*;
import facebook4j.auth.AccessToken;
import facebook4j.auth.OAuthAuthorization;
import facebook4j.auth.OAuthSupport;
import facebook4j.conf.Configuration;
import facebook4j.conf.ConfigurationBuilder;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.Thread.sleep;

/**
 * Created by werni on 22/09/15.
 */
public class FacebookStatusSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<String> queue;
    private Facebook facebookClient;

    private Date pivotDate;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {


        ConfigurationBuilder confBuilder = new ConfigurationBuilder();

        confBuilder.setDebugEnabled(true);
        confBuilder.setOAuthAppId("1030404896991063");
        confBuilder.setOAuthAppSecret("bcb5e7063e6d5300481c847de377e9d3");
        confBuilder.setUseSSL(true);
        confBuilder.setJSONStoreEnabled(true);
        Configuration configuration = confBuilder.build();
        FacebookFactory facebookFactory = new FacebookFactory(configuration);
        facebookClient = facebookFactory.getInstance();
        AccessToken accessToken = null;
        try {
            OAuthSupport oAuthSupport = new OAuthAuthorization(configuration);
            accessToken = oAuthSupport.getOAuthAppAccessToken();

        } catch (FacebookException e) {
            System.err.println("Error while creating access token " + e.getLocalizedMessage());
        }
        facebookClient.setOAuthAccessToken(accessToken);
//results in an error says {An active access token must be used to query information about the current user}


        collector = spoutOutputCollector;

        queue = new LinkedBlockingQueue<String>();
        pivotDate = new Date(0);
    }


    public void fillQueue() {

        //String uri = entry.getUri();
        //Date date = entry.getPublishedDate();

            List<Post> posts = null;
            try {
                posts = facebookClient.getFeed("JustinBieber");
            } catch (FacebookException e) {
                e.printStackTrace();
            }
            Collections.sort(posts, new Comparator<Post>() {
                public int compare(Post o1, Post o2) {
                    return o1.getCreatedTime().compareTo(o2.getCreatedTime());
                }
            });
            for (Post p : posts) {
                    //System.out.println("p" + p.getCreatedTime().toString());
                    //System.out.println(pivotDate.toString());
                if (p.getCreatedTime() != null && p.getMessage() != null && pivotDate.before(p.getCreatedTime())) {

                    pivotDate = p.getCreatedTime();
                    System.out.println(p.getMessage());
            //System.out.println(pivotDate.toString());
                    //System.out.println(feed.getTitle().toString() + ": " + entry.getPublishedDate().toString() + " " + title);
                    queue.offer(p.getMessage());
                    try {
                        sleep(300);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                //feed.clear();
                // size = 1;
            }
            try {
                sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    @Override
    public void nextTuple() {
        try {
            sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String title = queue.poll();
        //System.out.println(title);
        if (title != null) {
            collector.emit(new Values(title));
        } else {
            fillQueue();
        }

        //feed.clear();
        // size = 1;

    }
}

