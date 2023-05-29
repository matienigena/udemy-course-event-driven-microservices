package com.microservices.demo.twitter.to.kafka.runner.impl;

import com.microservices.demo.twitter.to.kafka.config.TwitterServiceConfigData;
import com.microservices.demo.twitter.to.kafka.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "false", matchIfMissing = true)
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;
    private final TwitterServiceConfigData configData;

    private TwitterStream twitterStream;
    public TwitterKafkaStreamRunner(TwitterKafkaStatusListener twitterKafkaStatusListener, TwitterServiceConfigData configData) {
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
        this.configData = configData;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown() {
        if(twitterStream != null) {
            LOG.info("Closing twitter Stream!");
            twitterStream.shutdown();
        }
    }

    private void addFilter() {
        String[] keywords = configData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
    }
}
