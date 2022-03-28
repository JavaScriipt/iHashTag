package iHashTag;

import java.util.Properties;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class StreamKafkaConsumer{
	
   public static void main(String[] args) throws Exception {
	   Scanner teclado = new Scanner(System.in);
	   
	   System.out.println("------- BIENVENIDO AL CONSUMIDOR TWITTER-KAKFA -------");
	   System.out.println("------- Por favor, ingresa el Hashtag a seguir ------");
	   String topicName = teclado.nextLine();
	   System.out.println("------- Ha decidido usted stremear a: "+ topicName+" ------");
	   
	   teclado.close();
	   
      //Kafka consumer configuration settings
      //String topicName = args[0].toString();
      Properties props = new Properties();
      int size = 0;
      int offset = 0;
      String variables [] = {"-10", "-11"};
      
      props.put("bootstrap.servers", "localhost:9092");
      props.put("group.id", "test");
      //props.put("enable.auto.commit", "true");
      //props.put("auto.commit.interval.ms", "1000");
      //props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", 
         "org.apache.kafka.common.serialization.StringDeserializer");
      
      KafkaConsumer<String, String> consumer = new KafkaConsumer
         <String, String>(props);
      
      mongoHandler mongodbHandler = new mongoHandler(topicName);
      
      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe(Arrays.asList(topicName.replace("#", "")));
      
      //print the topic name
      System.out.println("Suscrito al topic: " + topicName.replace("#", ""));

      
      while (true) {
         ConsumerRecords<String, String> records = consumer.poll(0);
         
         for (ConsumerRecord<String, String> record : records) {
        	 
	         if(size < 100) {
	        	 mongodbHandler.saveDocument(record.value(), variables[offset]);
	        	 System.out.println("--- Documento guardado en MongoDB");
	        	 size += 1;
	         }else {
	        	 size = 0;
	        	 offset +=1;
	        	 if(offset < 2) {
	        		 mongodbHandler.saveDocument(record.value(), variables[offset]);
	        		 System.out.println("--- Documento guardado en MongoDB");
	        		 size += 1;
	        	 }else {
	        		 offset = 0;
	        		 mongodbHandler.saveDocument(record.value(), variables[offset]);
	        		 System.out.println("--- Documento guardado en MongoDB");
	        		 size += 1; 
	        	 }
	         }
         }      
      }
   }
}
