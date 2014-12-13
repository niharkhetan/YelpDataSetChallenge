package task2;

/**
 * @author Bipra De
 * This code computes the recommended and non-recommended menu items of restaurants
 * Date : December 5th, 2014
 */
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class GiveRecommendations {
	
	private MongoClient mongoClient;
	private DB db;
	private DBCollection featureCollection;
	private DBCollection businessCollection;
	private DBCollection testCollection;
	private DBCollection trainingCollection;
	
	GiveRecommendations(MongoClient mongoClient,DB db,DBCollection featureCollection,DBCollection businessCollection,DBCollection testCollection,DBCollection trainingCollection)
	{
		this.mongoClient=mongoClient;
		this.db=db;
		this.featureCollection=featureCollection;
		this.businessCollection=businessCollection;
		this.testCollection=testCollection;
		this.trainingCollection=trainingCollection;
		
	}

	public static void main(String[] args) throws IOException
	{
		
		//Defining required parameters to create an object of the "GiveRecommendations" class
		
		//Prepare connection to MongoDB with the YELP database
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelp" );
		
		//"feature_set_for_restaurants" collection stores the top N features for each business belonging to Restaurant categories
		DBCollection featureCollection = db.getCollection("feature_set_for_restaurants");
		
		//"business" collection stores the business data provided in the yelp data set. 
		DBCollection businessCollection = db.getCollection("business");
		
		//"test_set" is the training collection (40% of total data) and "training_set" is the training collection (60% of total data)
		DBCollection testCollection = db.getCollection("test_set");
		DBCollection trainingCollection = db.getCollection("training_set");
		
		GiveRecommendations computeRecommendations=new GiveRecommendations(mongoClient,db,featureCollection,businessCollection,testCollection,trainingCollection);
		computeRecommendations.computeRecommendedAndNonRecommendedMenuItems(featureCollection,
				businessCollection, testCollection, trainingCollection);	
	}

	/***
	 * This method computes the recommended and not recommended menu items for restaurants based on POS tagging(extracting Nouns) and  Sentiment Analysis
	 * @param featureCollection
	 * @param businessCollection
	 * @param testCollection
	 * @param trainingCollection
	 * @throws IOException
	 */
	private  void computeRecommendedAndNonRecommendedMenuItems(
			DBCollection featureCollection, DBCollection businessCollection,
			DBCollection testCollection, DBCollection trainingCollection)
			throws IOException {
		
		//Declaring the variables required for MongoDB query operations
		DBObject queryString=new BasicDBObject();
		DBCursor featureCursor=featureCollection.find(queryString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		DBObject featureResult;
		DBObject searchString;
		DBObject updateValue;
		DBObject updateString;
		DBCursor testCursor;
		DBCursor trainingCursor;
		DBObject trainingQueryString;
		DBObject testQueryString;
		String businessID;
		String features;
		String[] featureList;
		List reviews;
		List tips;
		DBObject result;
		
		String reviewsAndTips = "";
		
		
		//Declaring the variables required to compute the recommended and not recommended menu items
		HashMap<String,String> recommendationData=new HashMap<String,String>();
		float sentimentIntensity;
		int sentimentVote;
		String[] recommendationDataValues;
		String positiveMenuItem;
		String negativeMenuItem;
		MenuFinder menuInformation=new MenuFinder();
		
		//Iterate over the documents in the "feature_set_for_restaurants" collection
		while (featureCursor.hasNext())
		{
				positiveMenuItem="";
				negativeMenuItem="";
				featureResult=featureCursor.next();
				businessID=(String) featureResult.get("business_id");
				features=((String) featureResult.get("features")).trim();
				featureList=features.split(" "); 
				
				trainingQueryString=new BasicDBObject("business_id",businessID);
				trainingCursor=trainingCollection.find(trainingQueryString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
				
				//Prepare concatenated string of reviews and tips of a business from training/test collection wherever it is present 
				if(trainingCursor.hasNext())
				{
					result=trainingCursor.next();
					reviews=(List)result.get("reviews");
					for (Object review:reviews)
						reviewsAndTips+=review.toString();
					tips=(List)result.get("tips");
					for (Object tip:tips)
						reviewsAndTips+=tip.toString();
				}
				else
				{
					testQueryString=new BasicDBObject("business_id",businessID);
					testCursor=testCollection.find(testQueryString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
					if(testCursor.hasNext())
					{
						result=testCursor.next();
						reviews=(List)result.get("reviews");
						for (Object review:reviews)
							reviewsAndTips+=review.toString();
						tips=(List)result.get("tips");
						for (Object tip:tips)
							reviewsAndTips+=tip.toString();
					}
				}
				
				
				
				//Pass reviewsAndTips to the "getMenu" method of "MenuFinder" class to extract the menu items along with their sentiment intensity and sentiment score
				recommendationData=menuInformation.getMenu(reviewsAndTips,Arrays.asList(featureList));
				
				//Iterating over each entry in the HashMap<String,String> returned by the "MenuFinder" class
				for(String menuItem:recommendationData.keySet())
				{
				
					recommendationDataValues=recommendationData.get(menuItem).split(" : ");
					
					//If the sentiment score is +ve then classify the menu item as "Recommended"
					if (Integer.parseInt(recommendationDataValues[1])>0)
					{
						positiveMenuItem+=menuItem+" [ Sentiment Intensity: "+recommendationDataValues[0]+", Sentiment Score: "+recommendationDataValues[1]+" ], ";
					}
					//If the sentiment score is -ve then classify the menu item as "Not Recommended"
					if (Integer.parseInt(recommendationDataValues[1])<0)
					{
						negativeMenuItem+=menuItem+" [ Sentiment Score: "+recommendationDataValues[0]+", Sentiment Score: "+recommendationDataValues[1]+" ], ";
					
					}
				}
				positiveMenuItem=positiveMenuItem.replaceAll(",$", "");
				negativeMenuItem=negativeMenuItem.replaceAll(",$", "");
				
				//Update the "business" collection with the recommended and not-recommended menu items
				searchString=new BasicDBObject("business_id",businessID);
				updateValue=new BasicDBObject("recommended_menu_tiems",positiveMenuItem).append("not_recommended_menu_items",negativeMenuItem);
				updateString=new BasicDBObject("$set",updateValue);
				businessCollection.update(searchString, updateString);
				reviewsAndTips="";
				
				
			
		}
	}


	
}
