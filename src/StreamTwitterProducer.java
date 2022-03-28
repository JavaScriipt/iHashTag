package iHashTag;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.ServerSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.google.gson.*;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class StreamTwitterProducer {

	/*
	 * Variables Stream Twitter
	 */
	public static final String _consumerKey = "";
	public static final String _consumerSecret = "";
	public static final String _accessToken = "";
	public static final String _accessTokenSecret = "";
    private static final boolean EXTENDED_TWITTER_MODE = true;

    
    
	public static void main(String[] args) {

		Scanner teclado = new Scanner(System.in);
		
		/*
		 * Configuración Productor Kafka
		 */
		
		System.out.println("------- BIENVENIDO AL PRODUCTOR TWITTER-KAKFA -------");
		System.out.println("------- Por favor, ingresa el Hashtag a seguir ------");
		String topicName = teclado.nextLine();
		System.out.println("------- Ha decidido usted stremear a: "+ topicName+" ------");
		teclado.close();
		
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		//If the request fails, the producer can automatically retry,
	    props.put("retries", 0);
	      
	    //Specify buffer size in config
	    //props.put("batch.size", 16384);
	      
	    //Reduce the no of requests less than 0   
	    //props.put("linger.ms", 1);
	      
	    //The buffer.memory controls the total amount of memory available to the producer for buffering.   
	    //props.put("buffer.memory", 33554432);
	      
	    props.put("key.serializer", 
	        "org.apache.kafka.common.serialization.StringSerializer");
	         
	    props.put("value.serializer", 
	       "org.apache.kafka.common.serialization.StringSerializer");
		
	    Producer<String, String> producer = new KafkaProducer<String,String>(props);
	    
		
		ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
		configurationBuilder.setOAuthConsumerKey(_consumerKey).setOAuthConsumerSecret(_consumerSecret)
				.setOAuthAccessToken(_accessToken).setOAuthAccessTokenSecret(_accessTokenSecret);

		
		TwitterStream twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();

		twitterStream.addListener(new StatusListener() {

			/* when a new tweet arrives */
			public void onStatus(Status status) {

				if (status.getRetweetedStatus() == null) {
					Gson gson = new Gson();
					String json_string = gson.toJson(status);
					
					producer.send(new ProducerRecord<String,String>(topicName.replace("#", ""),
							json_string));
					
					//System.out.println("Mensaje enviado");
					//System.out.println(status.getText());
					//System.out.println("TWEET SIN RETTWETEAR");

				}else {
					Gson gson = new Gson();
					String json_string = gson.toJson(status);
					
					producer.send(new ProducerRecord<String,String>(topicName.replace("#", ""),
							json_string ));
					//System.out.println("Mensaje enviado");
					//System.out.println(status.getText());
					//System.out.println("TWEET RETTWETEADO");

				}
				System.out.println("--------------------------------");
			}
			
			public String statusJsonImplToString(String tweet) {
				String final_string;
				
				final_string ="{content_tweet:"+tweet+"}";
				
				return final_string;
			}

			@Override
			public void onException(Exception arg0) {
				System.out.println("Exception on twitter");

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				System.out.println("Exception on twitter");

			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {
				System.out.println("onScrubGeo");

			}

			@Override
			public void onStallWarning(StallWarning arg0) {
				System.out.println("EonStallWarning");

			}

			@Override
			public void onTrackLimitationNotice(int arg0) {
				System.out.println("EonTrackLimitationNotice");

			}
		});

		FilterQuery tweetFilterQuery = new FilterQuery(); // See
		tweetFilterQuery.track(new String[] {topicName}).language("en"); // , "Teletubbies"}); // OR on keywords

		// ejemplo de localización  (desde USA)
		// tweetFilterQuery.locations(new double[][]{new  double[]{-126.562500,30.448674}, new double[]{-61.171875,44.087585 }});
		// See https://dev.twitter.com/docs/streaming-apis/parameters#locations for
		// proper location doc.
		// Note that not all tweets have location metadata set.
		// ejemplo de idioma  (en inglés)
		/* tweetFilterQuery.language(new String[]{"en"}); */ 
		twitterStream.filter(tweetFilterQuery);
	}
}
