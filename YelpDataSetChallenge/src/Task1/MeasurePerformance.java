package Task1;

import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MeasurePerformance {

	public static void main(String[] args) throws UnknownHostException
	{
		
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelp" );
		DBCollection testCollection = db.getCollection("test_set");
		DBCollection outputCollection = db.getCollection("categories_assigned_from_code");
		DBObject queryString=new BasicDBObject();
		DBCursor outputCursor=outputCollection.find(queryString);
		DBObject result;
		DBObject result1;
		DBCursor testCursor;
		DBObject queryString1;
		String businessID;
		List testCategories;
		String outputCategories;
		String[] generatedCategories;
		while(outputCursor.hasNext())
		{
			result=outputCursor.next();
			businessID=(String) result.get("business_id");
			outputCategories=(String) result.get("categories"); 
			generatedCategories=outputCategories.split(","); // Categories assigned by our code
			queryString1=new BasicDBObject("business_id",businessID);
			testCursor=testCollection.find(queryString1);
			if(testCursor.hasNext())
			{
				result1=testCursor.next();
				testCategories=(List) result1.get("categories"); //Ground truth categories
				int matched=0;
				float precision=0;
				float recall=0;
				float fMeasure=0;
				if (generatedCategories.length > testCategories.size())
				{	
					for(String generatedCategory:generatedCategories)
					{
						for(Object testCategory:testCategories )
						{
							if ((generatedCategory.trim()).equals(testCategory.toString().trim()))
							{
								matched++;
							}
									
						}
					}
				}
				else
				{
					for(Object testCategory:testCategories)
					{
						for(String generatedCategory:generatedCategories)
						{
							if ((generatedCategory.trim()).equals(testCategory.toString().trim()))
							{
								matched++;
							}
									
						}
					}
					
				}
				if (testCategories.size()>0) //i.e. The business id has some ground truth to measure precision and recall
				{
					
					precision=((float)matched/(generatedCategories.length));
					recall=((float)matched/(testCategories.size()));
					if(precision!=0 && recall!=0)
						fMeasure=((float) 2*precision*recall)/(precision+recall)*(5/4);
					else
						fMeasure=0;
					
					System.out.println("Business ID : "+businessID+" | Ground Truth Categories : "+testCategories.toString()+" | Programatically Assigned Categories : "+outputCategories+" | Precision : "+precision+" | Recall : "+recall+" | F-Measure: "+fMeasure);
//						System.out.println("Matched "+matched+" generated categories length "+generatedCategories.length+" test Categories size "+testCategories.size());
						
						
				}
			}
			
			
		}
	}
}
