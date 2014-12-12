package task2;

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

	public static void main(String[] args) throws IOException
	{
		
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelp" );
		DBCollection featureCollection = db.getCollection("feature_set_for_restaurants");
		DBCollection businessCollection = db.getCollection("business");
		DBCollection testCollection = db.getCollection("test_set");
		DBCollection trainingCollection = db.getCollection("training_set");
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
		String reviewsAndTips = "";
		List reviews;
		List tips;
		HashMap<String,String> recommendationData=new HashMap<String,String>();
		
		float sentimentIntensity;
		int sentimentVote;
		String[] recommendationDataValues;
		String positiveMenuItem;
		String negativeMenuItem;
		MenuFinder menuInformation=new MenuFinder();
		DBObject result;
		while (featureCursor.hasNext())
		{
			positiveMenuItem="";
			negativeMenuItem="";
			featureResult=featureCursor.next();
			businessID=(String) featureResult.get("business_id");
			//if(businessID.equals("RgDg-k9S5YD_BaxMckifkg")||businessID.equals("cruBFtsFaBuhX_I72uU8pA")||businessID.equals("xoc5751bEHVpWauIhUjbfQ")||businessID.equals("JwUE5GmEO-sH1FuwJgKBlQ")||businessID.equals("uGykseHzyS5xAMWoN6YUqA")||businessID.equals("LRKJF43s9-3jG9Lgx4zODg")||businessID.equals("rdAdANPNOcvUtoFgcaY9KA")||businessID.equals("zOc8lbjViUZajbY7M0aUCQ")|| businessID.equals("UgjVZTSOaYoEvws_lAP_Dw")||businessID.equals("suQyHycqv8nA7EualcUB3g")||businessID.equals("sCWO7Up6mjgbDNE6Wtd0wA")||businessID.equals("WcTu6LJsXa-tfRxAWzbopw")||businessID.equals("_wZTYYL7cutanzAnJUTGMA")||businessID.equals("SKLw05kEIlZcpTD5pqma8Q")||businessID.equals("77ESrCo7hQ96VpCWWdvoxg")||businessID.equals("KTqNU4plO23583DYAMGXYg")||businessID.equals("MKsb2VpLB-0UBODcInDsSw")||businessID.equals("ShEYKerTwb2LSORE5o_s7A")||businessID.equals("HaBkx5PwvbBpQ2iNCgHnVQ"))
				//{
					features=((String) featureResult.get("features")).trim();
				featureList=features.split(" "); //pass this list to Saurabh's code
				
				trainingQueryString=new BasicDBObject("business_id",businessID);
				trainingCursor=trainingCollection.find(trainingQueryString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
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
				//Pass reviewsAndTips to Saurabh's code as string
				recommendationData=menuInformation.getMenu(reviewsAndTips,Arrays.asList(featureList));
				//Call Saurabh's code with the features to get the sentiment scores for each features.
				
				for(String menuItem:recommendationData.keySet())
				{
				
					recommendationDataValues=recommendationData.get(menuItem).split(" : ");
				
				
				if (Integer.parseInt(recommendationDataValues[1])>0)
					positiveMenuItem+=menuItem+" [ Sentiment Intensity: "+recommendationDataValues[0]+", Sentiment Score: "+recommendationDataValues[1]+" ], ";
				if (Integer.parseInt(recommendationDataValues[1])<0)
					negativeMenuItem+=menuItem+" [ Sentiment Score: "+recommendationDataValues[0]+", Sentiment Score: "+recommendationDataValues[1]+" ], ";
				}
				positiveMenuItem=positiveMenuItem.replaceAll(",$", "");
				negativeMenuItem=negativeMenuItem.replaceAll(",$", "");
				
				searchString=new BasicDBObject("business_id",businessID);
				updateValue=new BasicDBObject("recommended_menu_tiems",positiveMenuItem).append("not_recommended_menu_items",negativeMenuItem);
				updateString=new BasicDBObject("$set",updateValue);
				businessCollection.update(searchString, updateString);
				reviewsAndTips="";
				System.out.println("HELLO "+businessID);
				
			//}
		}	
	}


	
}
