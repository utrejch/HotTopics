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

public class FacebookTelcoSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<String> queue;
    private Facebook facebookClient;
    private String[] sources;
    private Date pivotDate;
    private HashMap<String, Date> cpivotDate;
    private int lag;
    Date endDate;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(0);
        cal.set(2015, 10, 17, 0, 0, 0);
        endDate = cal.getTime();

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
        lag = 1;
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
        cpivotDate = new HashMap<String, Date>();
        sources = new String [] {"youseedanmark", "tdc", "teliadanmark", "cbbmobil", "Bibobmobil",
                        "TELMORE", "fullratedk", "Callme", "bedst.til.prisen"};

        //sources = new String [] { "telenordanmark"};
    }


    public void fillQueue() {

        //String uri = entry.getUri();
        //Date date = entry.getPublishedDate();
        for(int i = 0; i < sources.length; i++){
            List<Post> posts = null;
            try {
                posts = facebookClient.getFeed(sources[i], new Reading().fields("posts,from,comments,likes"));
            } catch (FacebookException e) {
                e.printStackTrace();
            }
                /*
                Collections.sort(posts, new Comparator<Post>() {
                    public int compare(Post o1, Post o2) {
                        return o1.getCreatedTime().compareTo(o2.getCreatedTime());
                    }
                });
                */
            for (Post p : posts) {
                //System.out.println("p" + p.getCreatedTime().toString());
                //System.out.println(pivotDate.toString());
                if (p.getCreatedTime() != null && p.getMessage() != null  && endDate.before(p.getCreatedTime())) {
                    pivotDate = p.getCreatedTime();
                    //System.out.println(p.getMessage().toLowerCase());
                    //System.out.println(pivotDate.toString());
                    //System.out.println(feed.getTitle().toString() + ": " + entry.getPublishedDate().toString() + " " + title);
                    queue.offer(p.getMessage());
                }
                List<Comment> comments = p.getComments();

                Collections.sort(comments, new Comparator<Comment>() {
                    public int compare(Comment o1, Comment o2) {
                        return -1 * o2.getCreatedTime().compareTo(o1.getCreatedTime());
                    }
                });
                for (Comment c : comments) {
                    Date cpivot = getCommentPivot(p.getId());
                    if (c.getCreatedTime() != null && c.getMessage() != null && endDate.before(c.getCreatedTime())) {
                        setCommentPivot(p.getId(), c.getCreatedTime());
                        //System.out.println("_____" + sources[i]);
                        System.out.println("c " + cpivot.toString());
                        System.out.println(c.getCreatedTime().toString());
                        //System.out.println(feed.getTitle().toString() + ": " + entry.getPublishedDate().toString() + " " + title);
                        String ctext = c.getMessage().toLowerCase().replace("\n", " ").replace("\t", " ");
                        ctext = ctext.replaceAll("[^\\p{L}\\p{Nd}]+", " ");
                        //System.out.println(ctext);
                        queue.offer(ctext);
                    }
                }
            }
        }
    }


    @Override
    public void nextTuple() {
        try {
            sleep(100);
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

    private void setCommentPivot(String id, Date d){
        cpivotDate.put(id, d);
    }

    private Date getCommentPivot(String id) {
        Date res = new Date(0);
        if (cpivotDate.containsKey(id)){
            Calendar cal = Calendar.getInstance(); // creates calendar
            cal.setTime(cpivotDate.get(id)); // sets calendar time/date
            cal.add(Calendar.HOUR_OF_DAY, lag); // adds one hour
            res = cal.getTime(); // returns new date object, one hour in the future
        }

        return res;
    }
}

