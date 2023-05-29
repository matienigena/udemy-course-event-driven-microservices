package com.microservices.demo.twitter.to.kafka.runner.impl;

import com.microservices.demo.twitter.to.kafka.config.TwitterServiceConfigData;
import com.microservices.demo.twitter.to.kafka.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MockStreamRunner.class);

    private final TwitterServiceConfigData twitterServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[] {
            "lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetuer"
    };

    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}" ;

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockStreamRunner(TwitterServiceConfigData twitterServiceConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterServiceConfigData = twitterServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        Integer minTweetLength = twitterServiceConfigData.getMockMinTweetLength();
        Integer maxTweetLength = twitterServiceConfigData.getMockMaxTweetLength();
        Long sleepTimeMs = twitterServiceConfigData.getMockSleepMs();
        LOG.info("Starting to mock");
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keywords, Integer minTweetLength, Integer maxTweetLength, Long sleepTimeMs) throws TwitterException {
        Executors.newSingleThreadExecutor().submit(() -> {
            while(true) {
                String formattedTweetAsRawJson = getFormattedString(keywords, minTweetLength, maxTweetLength);
                Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                twitterKafkaStatusListener.onStatus(status);
                sleep(sleepTimeMs);
            }
        });
    }

    private String getFormattedString(String[] keywords, Integer minTweetLength, Integer maxTweetLength) {
        String[] params = new String[] {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
        };

        return formatTweetAsJsonWithParams(params);
    }

    private static String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, Integer minTweetLength, Integer maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private static String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i == tweetLength /2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }


    private void sleep(Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch(InterruptedException e) {
            // TODO: add custom exception
            throw new RuntimeException("Error while sleeping for more tweets");
        }
    }
}
