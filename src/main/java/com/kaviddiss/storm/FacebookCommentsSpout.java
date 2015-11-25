package com.kaviddiss.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import facebook4j.*;
import facebook4j.auth.AccessToken;
import facebook4j.auth.OAuthAuthorization;
import facebook4j.auth.OAuthSupport;
import facebook4j.conf.Configuration;
import facebook4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.Thread.sleep;

/**
 * Created by werni on 22/09/15.
 */

public class FacebookCommentsSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<String> queue;
    private Facebook facebookClient;
    private int lag;
    private Date pivotDate;
    private HashMap<String, Date> cpivotDate;

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
        lag = 1;
        queue = new LinkedBlockingQueue<String>();
        pivotDate = new Date(0);
        cpivotDate = new HashMap<String, Date>();


    }


    public void fillQueue() {

        //String uri = entry.getUri();
        //Date date = entry.getPublishedDate();

        List<Post> posts = null;
        try {
            posts =  facebookClient.getFeed("cnn", new Reading().fields("posts,from,comments,likes").filter("stream"));
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
        if(posts != null) {
            for (Post p : posts) {
                //System.out.println("p" + p.getCreatedTime().toString());
                //System.out.println(pivotDate.toString());
                if (p.getCreatedTime() != null && p.getMessage() != null) {// && pivotDate.before(p.getCreatedTime())) {
                    pivotDate = p.getCreatedTime();
                    System.out.println(p.getMessage().toLowerCase());
                    //System.out.println(pivotDate.toString());
                    //System.out.println(feed.getTitle().toString() + ": " + entry.getPublishedDate().toString() + " " + title);
                    queue.offer(p.getMessage());
                }

                PagableList<Comment> comments = p.getComments();
                Paging<Comment> pagingC;
                try {
                    do {

                        Collections.sort(comments, new Comparator<Comment>() {
                            public int compare(Comment o1, Comment o2) {
                                return -1 * o2.getCreatedTime().compareTo(o1.getCreatedTime());
                            }
                        });
                        for (Comment c : comments) {
                            Date cpivot = getCommentPivot(p.getId());
                            if (c.getCreatedTime() != null && c.getMessage() != null && cpivot.before(c.getCreatedTime())) {
                                setCommentPivot(p.getId(), c.getCreatedTime());
                                System.out.println("c " + cpivot.toString());
                                System.out.println(c.getCreatedTime().toString());
                                //System.out.println(feed.getTitle().toString() + ": " + entry.getPublishedDate().toString() + " " + title);
                                String ctext = c.getMessage().toLowerCase().replace("\n", "").replace("\t", "");
                                ctext = ctext.replaceAll("[^\\p{L}\\p{Nd}]+", " ");
                                System.out.println(ctext);
                                queue.offer(ctext);
                            }
                        }
                        pagingC = comments.getPaging();
                    }
                    while ((pagingC != null) &&
                            ((comments = facebookClient.fetchNext(pagingC)) != null));
                } catch (FacebookException e) {
                    e.printStackTrace();
                }

            }
        } else {System.out.println("posts is null");}
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
            cal.add(Calendar.HOUR_OF_DAY, -lag); // adds one hour
            res = cal.getTime(); // returns new date object, one hour in the future
       }

       /*
        if (cpivotDate.containsKey(id)){
            res = cpivotDate.get(id);
        }
        */
        return res;
    }
}

