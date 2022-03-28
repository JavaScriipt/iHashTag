package iHashTag;

import java.util.Scanner;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class main {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Scanner teclado = new Scanner(System.in);
		   
		System.out.println("------- BIENVENIDO AL MAIN -------");
		System.out.println("------- Por favor, ingresa el Hashtag a seguir ------");
		String topicName = teclado.nextLine();
		System.out.println("------- Ha decidido usted stremear a: "+ topicName+" ------");
		teclado.close();
		
		MongoClient mongo = new MongoClient("localhost",27017);
		MongoDatabase database = mongo.getDatabase("cdistribuida");
		MongoCollection collection = database.getCollection(topicName);
		
		MongoDatabase database2 = mongo.getDatabase("dashdata");
		MongoCollection collection2 = database2.getCollection(topicName);
		collection2.drop();
		collection2 = database2.getCollection(topicName);
		
		Document document_data = new Document();
		document_data.put("muynegativo", "X");
		document_data.put("negativo", "X");
		document_data.put("neutro", "X");
		document_data.put("positivo", "X");
		document_data.put("muypositivo", "X");
		document_data.put("average", "X");
		

		collection2.insertOne(document_data);
		
		while(true) {
			sentiment_average(collection, collection2);
			System.out.println("Vamos progresando");
		}
		
			
	}
	
	public static Float count_query(MongoCollection collection, String query1, String query2) {
		Float final_count = new Float(0.0);
		BasicDBObject query = new BasicDBObject();
		query.put(query1, query2);
		
		FindIterable<Document> search = collection.find(query);
		for (Document doc : search) {
			final_count+=1;
		}	
		
		return final_count;
	};
	
	public static Float sentiment_average(MongoCollection collection, MongoCollection collection2) {
		Float total;
		Float nominador;
		Float denominador;
		Float cantidadN2;
		Float cantidadN1;
		Float cantidad0;
		Float cantidadP1;
		Float cantidadP2;
		String id;
		
		cantidadN2 = count_query(collection,"sentiment","-2");
		cantidadN1 = count_query(collection,"sentiment","-1");
		cantidad0 = count_query(collection,"sentiment","0");
		cantidadP1 = count_query(collection,"sentiment","1");
		cantidadP2 = count_query(collection,"sentiment","2");
		
		nominador = 3*cantidadP2 + 2*cantidadP1 + cantidad0 - cantidadN1 - 2*cantidadN2;
		denominador = cantidadP2 + cantidadP2 + cantidadN1 + cantidadN2 + cantidad0;
		total = nominador/denominador;
		
		FindIterable<Document> documento = collection2.find();
		for (Document doc : documento) {
			System.out.println("Hola");
			JsonObject jsonObject = new Gson().fromJson(doc.toJson(),JsonObject.class);
			JsonObject jsonObject_id = (JsonObject) jsonObject.get("_id");
			id = jsonObject_id.get("$oid").toString().replace("\"","");
			
			BasicDBObject newDocument = new BasicDBObject();
	     	newDocument.append("$set", new BasicDBObject().append("muynegativo", cantidadN2.toString()).
	     			append("negativo",cantidadN1.toString()).append("neutro", cantidad0.toString()).
	     			append("positivo",cantidadP1.toString()).append("muypositivo", cantidadP2.toString()).
	     			append("average",total.toString())
	     			);

	     	ObjectId objectid = new ObjectId(id);
	     	BasicDBObject searchQuery = new BasicDBObject().append("_id", objectid);

	     	collection2.updateOne(searchQuery, newDocument);
		}
		
		return total;
	};
}
