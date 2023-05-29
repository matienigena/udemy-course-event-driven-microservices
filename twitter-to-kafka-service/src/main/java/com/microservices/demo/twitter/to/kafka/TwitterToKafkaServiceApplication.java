package com.microservices.demo.twitter.to.kafka;

import com.microservices.demo.twitter.to.kafka.config.TwitterServiceConfigData;
import com.microservices.demo.twitter.to.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import twitter4j.TwitterException;

import java.util.Arrays;

@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
    private final TwitterServiceConfigData twitterServiceConfigData;
    private final StreamRunner streamRunner;

    public TwitterToKafkaServiceApplication(TwitterServiceConfigData twitterServiceConfigData,
                                             StreamRunner streamRunner) {
        this.twitterServiceConfigData = twitterServiceConfigData;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class);
    }

    @Override
    public void run(String... args) throws TwitterException {
        LOG.info(twitterServiceConfigData.getWelcomeMessage());
        LOG.info(Arrays.toString(twitterServiceConfigData.getTwitterKeywords().toArray(new String[] {})));
        streamRunner.start();
    }
}
