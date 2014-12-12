/***
 * @author biprade
 * This code is used for performance evaluation by comparing the results generated by code against the ground-truth data.
 * Date : December 4th, 2014
 */
package task1;

import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MeasurePerformance {

	private MongoClient mongoClient;
	private DB db;
	private DBCollection testCollection;
	private DBCollection outputCollection;
	MeasurePerformance(MongoClient mongoClient,DB db,DBCollection testCollection,DBCollection outputCollection)
	{
		this.mongoClient=mongoClient;
		this.db=db;
		this.testCollection=testCollection;
		this.outputCollection=outputCollection;
	}
	public static void main(String[] args) throws UnknownHostException
	{
		//Defining required parameters to create an object of the "MeasurePerformance" class
		//Prepare connection to MongoDB with the YELP database
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelp" );
		
		//"test_set" is the collection name of the test data
		DBCollection testCollection = db.getCollection("test_set");
		//"categories_assigned_from_code" stores the categories assigned programmatically for each business ID in the test collection
		DBCollection outputCollection = db.getCollection("categories_assigned_from_code");
		
		MeasurePerformance evaluator=new MeasurePerformance(mongoClient,db,testCollection,outputCollection);
		evaluator.generateEvaluationMetrics(testCollection, outputCollection);
	}

	/**
	 * This method is used to prepare the evaluation metrics (Precision,Recall and F2-measure for businesses in the test data
	 * @param testCollection
	 * @param outputCollection
	 */
	private  void generateEvaluationMetrics(DBCollection testCollection,
			DBCollection outputCollection) {
		
		//Declaring variable for MongoDB query operations
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
		
		//Iterating over each document in the "categories_assigned_from_code" collection that stores the programmatically assigned categories for each business in test data
		while(outputCursor.hasNext())
		{
			result=outputCursor.next();
			businessID=(String) result.get("business_id");
			outputCategories=(String) result.get("categories"); 
			
			// Categories assigned by our code
			generatedCategories=outputCategories.split(","); 
			
			queryString1=new BasicDBObject("business_id",businessID);
			testCursor=testCollection.find(queryString1);
			if(testCursor.hasNext())
			{
				result1=testCursor.next();
				//Ground truth categories
				testCategories=(List) result1.get("categories"); 
				
				//Compute the number of matched categories between the programmatically assigned and ground truth categories
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
					//Compute Precision,Recall and F2 measure
					precision=((float)matched/(generatedCategories.length));
					recall=((float)matched/(testCategories.size()));
					if(precision!=0 && recall!=0)
						fMeasure=((float) 2*precision*recall)/(precision+recall)*(5/4);
					else
						fMeasure=0;
					
					System.out.println("Business ID : "+businessID+" | Ground Truth Categories : "+testCategories.toString()+" | Programatically Assigned Categories : "+outputCategories+" | Precision : "+precision+" | Recall : "+recall+" | F-Measure: "+fMeasure);

						
						
				}
			}
			
			
		}
	}
}
