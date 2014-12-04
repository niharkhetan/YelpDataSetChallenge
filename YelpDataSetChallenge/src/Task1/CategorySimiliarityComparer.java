package Task1;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class CategorySimiliarityComparer {



	public  HashMap<String, ArrayList<String>> returnGroupedCollections() throws UnknownHostException {
		HashMap<String,String> categoryFeatureSet = new HashMap<>();
		HashMap<String, String[]> mapWithSplitStrings = new HashMap<>();
		HashMap<String, ArrayList<String>> groupings = new HashMap<>();
		
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelp" );
		DBCollection featureCollection = db.getCollection("feature_set");
		DBObject queryString=new BasicDBObject();
		DBCursor cursor=featureCollection.find(queryString);
		DBObject result;
		while (cursor.hasNext())
		{
			result=cursor.next();
			categoryFeatureSet.put(result.get("category").toString(),result.get("features").toString());
		}
		
		
//		categoryFeatureSet.put("Chinese", "soup noodles pan asian manchow pad thai rice cuisine padthad");
//		categoryFeatureSet.put("Thai", "soup noodles lomo dalai bhutto gama rice cuisine padthad");
//		categoryFeatureSet.put("Indonese", "soup noodles lomo dalai bhutto ppop corns danmein pojo kims");
//		categoryFeatureSet.put("Indian", "dosa indli sambhar chicken paratha pulav paneer kheer rasgulla mithai");
//		categoryFeatureSet.put("Kashmiri", "sambhar chicken paratha pulav paneer khus podam roganjosh cuisine padthad");
		
		for (String key : categoryFeatureSet.keySet()) {
		    String value = categoryFeatureSet.get(key);
		    String[] words = value.split("\\s+");		    
		    mapWithSplitStrings.put(key, words);
		}

		Integer countCommon;
		for (String keyA : mapWithSplitStrings.keySet()) {
		    String[] valueSetA = mapWithSplitStrings.get(keyA);
		    ArrayList<String> commonSet = new ArrayList<>();
		   
		    for (String keyB : mapWithSplitStrings.keySet()){
			    if (keyB != keyA){
			    	String valueSetB[] = mapWithSplitStrings.get(keyB);
			    	countCommon = findCommon(valueSetA, valueSetB);			    		
			    	if (countCommon > 6){
			    		commonSet.add(keyB);
			    	}
			    }
			}
		    groupings.put(keyA,commonSet);
		}
		
//		for (String key : groupings.keySet()){
//			System.out.println("Key = " + key + ", Value = " + groupings.get(key));
//		}
		return groupings;
	}
	
	public  Integer findCommon(String[] listA, String[] listB){
		String arrayToHash[] = listA;
		String arrayToSearch[] = listB;
		
		HashSet<String> intersection = new HashSet<>();
		HashSet<String> hashedArray = new HashSet<>();
		
		for( String entry : arrayToHash){
			hashedArray.add(entry);
		}
		
		for( String entry : arrayToSearch){
			if(hashedArray.contains(entry)){
				intersection.add(entry);
			}
		}
		
		return intersection.size();
	}
}
