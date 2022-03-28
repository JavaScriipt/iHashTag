package iHashTag;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import org.bson.Document;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class mongoHandler {
	MongoClient mongo;
	MongoDatabase database;
	MongoCollection collection;
	
	public mongoHandler(String topicname) {
		this.mongo = new MongoClient("localhost",27017);
		this.database = mongo.getDatabase("cdistribuida");
		collection = this.database.getCollection(topicname);
	}
	
	public void saveDocument(String tweet, String sentiment_value) throws JsonParseException, JsonMappingException, IOException{
		
		HashMap<String,Object> json =
		        new ObjectMapper().readValue(tweet, HashMap.class);
		
		
		Document document = new Document( "timestamp" ,getKeyMongo( json.get("createdAt").toString()));
		document.put("sentiment", sentiment_value);
		document.put("tweet", json);
		
		collection.insertOne(document);
		//System.out.println(document.isEmpty());
		
	}
	
	public String getKeyMongo(String Date) {
		String final_key;
		String parts[];
		String aux_parts[];
		
		parts = Date.split(" ");
		aux_parts = parts[1].split(",");
		parts[1] = aux_parts[0];

		
		if(parts[0].contentEquals("Jan")) {
			parts[0] = "1";
		}
		//debe seguir con los demás meses(Pero no se como aparecen en Twitter)
		
		aux_parts = parts[parts.length -2 ].split(":");
		
		if(parts[4].contentEquals("PM")) {
			int hora = Integer.parseInt(aux_parts[0]) + 12;
			aux_parts[0] = Integer.toString(hora);
		}
		
		final_key = parts[1]+"_"+parts[0]+"_"+parts[2]+"_"+aux_parts[0]+"_"+aux_parts[1];
				
		return final_key;
	}
	
}
