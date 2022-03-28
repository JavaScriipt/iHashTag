package iHashTag;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import com.google.gson.*;


import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import org.bson.Document;
import org.bson.types.ObjectId;


public class SentimentMongo {
	
	//@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		Scanner teclado = new Scanner(System.in);
		   
		System.out.println("------- BIENVENIDO AL SENTIMENT TWEET (1) -------");
		System.out.println("------- Por favor, ingresa la collection MongoDB correspondiente ------");
		String topicName = teclado.nextLine();
		System.out.println("------- Bind a la Collection: "+ topicName+" ------");
		teclado.close();
		

		MongoClient mongoClient = new MongoClient();
		MongoDatabase database = mongoClient.getDatabase("cdistribuida");
		MongoCollection<Document> collection = database.getCollection(topicName);
		
		TweetTreatment tweet_treatment = new TweetTreatment();
		SentimentAnalyzer sentimentAnalyzer = new SentimentAnalyzer();
		List<TweetWithSentiment> sentiments = new ArrayList<>();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		
		Properties properties = new Properties();
	   	properties.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
	    StanfordCoreNLP stanfordCoreNLP = new StanfordCoreNLP(properties);
		
		String document_jsonstring;
		String id;
		String sentiment_value;
		String clean_tweet;
		String dirty_tweet;

		
		/**Block<Document> printBlock = new Block<Document>() {
		       @Override
		       public void apply(final Document document) {
		           System.out.println(document.toJson());
		       }
		};**/
		
		//SPARK
	
		
		//ANTERIOR
		while(true) {
			FindIterable<Document> prueba = collection.find(eq("sentiment", "-10"));
			int i = 0;
			for (Document document : prueba) {
				i++;
				sentiment_value = "-15";
				JsonObject jsonObject = new Gson().fromJson(document.toJson(),JsonObject.class);
				JsonObject jsonObject_id = (JsonObject) jsonObject.get("_id");
				JsonObject jsonObject_tweet = (JsonObject) jsonObject.get("tweet");
				id = jsonObject_id.get("$oid").toString().replace("\"","");
				
				if (jsonObject_tweet.get("retweetedStatus") == null) {
					dirty_tweet = jsonObject_tweet.get("text").toString();
	
				}else {
					JsonObject jsonObjectaux;
					jsonObjectaux = (JsonObject) jsonObject_tweet.get("retweetedStatus");
					dirty_tweet = jsonObjectaux.get("text").toString();
				}
				
				
				
		         clean_tweet = tweet_treatment.tweet_cleaner(dirty_tweet);
		         //System.out.println(clean_tweet);
		         TweetWithSentiment tweetWithSentiment = sentimentAnalyzer.findSentiment(clean_tweet, stanfordCoreNLP);
		         if (tweetWithSentiment != null) {
				        sentiments.add(tweetWithSentiment);
				        sentiment_value = tweetWithSentiment.getCssClass();
				    }
		         
		         
		         //Hacemos UPDATE
		         
		        BasicDBObject newDocument = new BasicDBObject();
		     	newDocument.append("$set", new BasicDBObject().append("sentiment", sentiment_value));
	
		     	ObjectId objectid = new ObjectId(id);
		     	BasicDBObject searchQuery = new BasicDBObject().append("_id", objectid);
	
		     	collection.updateOne(searchQuery, newDocument);
	
			}
		}
	}


}
