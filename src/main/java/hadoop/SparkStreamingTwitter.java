package hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.ConfigurationBuilder;

public class SparkStreamingTwitter {
    public static void main(String[] args) throws InterruptedException {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setOAuthConsumerKey("V06F9Q7VOg3KkrWudC06iLKCb");
        builder.setOAuthConsumerSecret("WQ3NG62eYxvOyG4l9S6kv0js1sM3WDEuNyFBR7HanJFN9r2ESt");
        builder.setOAuthAccessToken("86460677-a7iVXlaa4Bcly3PXbl3AdtyCKV8XJLclHdFDNylLl");
        builder.setOAuthAccessTokenSecret("Avw6TN4uHpTurENHojwgLBtaqBtYN79qjZhwaj557R6ma");
        TwitterFactory tf = new TwitterFactory(builder.build());
        Twitter twitter = tf.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitter.getConfiguration());
        SparkConf sparkConf = new SparkConf().setAppName("Tweets-Analysis").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(1000));
        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(sc, twitterAuth);
        //tweets.print();
        JavaDStream<Status> z = tweets.map((Function<Status, Status>) s -> s);
        JavaDStream<String> w = tweets.map((Function<Status, String>) Status::getText);
        JavaDStream<String> x = w.filter((Function<String, Boolean>) tweet -> tweet.contains("fuck"));
        //JavaDStream<String> words = tweets.flatMap((FlatMapFunction<Status, String>) s -> Arrays.asList(s.getText().split(" ")).iterator());
        /*JavaDStream<String> w = tweets.map(new Function<Status, String>() {
            @Override
            public String call(Status status) throws Exception {
                System.out.println(status.getCreatedAt()+" "+status.getUser().getName()+" "+status.getText());
                return null;
            }
        });*/
        x.print();
        //words.print(10);
        //JavaDStream<String> hashTags = words.filter((Function<String, Boolean>) word -> word.startsWith("#"));
        //JavaDStream<String> hashTags = words.filter((Function<String, Boolean>) word -> word.contains("Ukraine"));
        //hashTags.print();
        //tweets.foreachRDD((VoidFunction<JavaRDD<>>));
        //JavaDStream<String> result = tweets.filter((Function<Status,String>) tweet -> tweet.getCreatedAt().);
        //result.print();
        sc.start();
        sc.awaitTermination();
    }
}
