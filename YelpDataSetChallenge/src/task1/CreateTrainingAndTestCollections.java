package task1;
/***
 * @author Bipra De, Nihar Khetan, Satvik Shetty, Anand Saurabh
 * This class is used to create the training and test collections.
 * Date : November 28th, 2014
 */
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class CreateTrainingAndTestCollections {
	
	private MongoClient mongoClient;
	private DB db;
	private DBCollection trainingCollection;
	private DBCollection testCollection;
	CreateTrainingAndTestCollections(MongoClient mongoClient,DB db,DBCollection trainingCollection,DBCollection testCollection)
	{
		this.mongoClient=mongoClient;
		this.db=db;
		this.trainingCollection=trainingCollection;
		this.testCollection=testCollection;
	}

	public static void main(String args[]) throws UnknownHostException
	{
		//Defining required parameters to create an object of the "CreateTrainingAndTestCollections" class
		
		//Prepare connection to MongoDB with the YELP database
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelp" );
		
		//Defining the training and test set
		DBCollection trainingCollection = db.getCollection("training_set");
		DBCollection testCollection = db.getCollection("test_set");
		
		//Create Index
		CreateTrainingAndTestCollections collectionsGenerator=new CreateTrainingAndTestCollections(mongoClient,db,trainingCollection,testCollection);
		collectionsGenerator.createTrainingAndTestCollections(db, trainingCollection, testCollection);
		
		}
/**
 * This method is used to create the training and test collection from the YELP dataset
 * @param db
 * @param trainingCollection
 * @param testCollection
 */
	private  void createTrainingAndTestCollections(DB db,
			DBCollection trainingCollection, DBCollection testCollection) {
		
		//Using the exisitng collections to prepare the training and test set
		DBCollection businessCollection = db.getCollection("business");
		DBCollection reviewsCollection = db.getCollection("reviews");
		DBCollection tipsCollection = db.getCollection("tips");
		
		//Declaring variables for MongoDB query operations
		DBObject projectionString=new BasicDBObject("_id",0).append("business_id",1).append("categories",1).append("name",1);
		DBCursor businessCursor=businessCollection.find(new BasicDBObject(),projectionString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		DBCursor reviewsCursor;
		DBObject reviewQueryString;
		DBCursor tipsCursor;
		DBObject tipsQueryString;
		int numberOfBusinessDocumentsProcessed=0;
		String businessID;
		DBObject businessObject;
		DBObject reviewsObject;
		DBObject tipsObject = null;
		
		
		//Iterating over business collection
		while (businessCursor.hasNext())
		{
			businessObject=businessCursor.next();
			
			
			//Added business collection's data to the BSON Object
			DBObject document=new BasicDBObject(businessObject.toMap());
			
			
			businessID=(String)businessObject.get("business_id");
			List reviews = new ArrayList();
			List reviewStars = new ArrayList();
			List tips = new ArrayList();
			List tipsLikes = new ArrayList();
			reviewQueryString=new BasicDBObject("business_id",businessID);
			reviewsCursor=reviewsCollection.find(reviewQueryString);
			while (reviewsCursor.hasNext())
			{
				reviewsObject=reviewsCursor.next();
				reviews.add((String) reviewsObject.get("text"));
				if (null==reviewsObject.get("stars"))
					reviewStars.add(0);
				else
					reviewStars.add((int)reviewsObject.get("stars"));
				
			}
			//Adding reviews collection's data to the BSON Object
			document.put("reviews",reviews );
			document.put("reviewStars", reviewStars);
			
			
			tipsQueryString=new BasicDBObject("business_id",(String)businessObject.get("business_id"));
			tipsCursor=tipsCollection.find(tipsQueryString);
			while (tipsCursor.hasNext())
			{
				tipsObject=tipsCursor.next();
				tips.add((String) tipsObject.get("text"));
				if(null==tipsObject.get("likes"))
					tipsLikes.add(0);
				else
					tipsLikes.add((int)tipsObject.get("likes"));
				
			}
			//Adding tips collection's data to the BSON Object
			document.put("tips",tips );
			document.put("tipsLikes", tipsLikes);
			
			//60% of the total data i.e. 25292 documents goes in the training collection and the rest in test collection
			if (numberOfBusinessDocumentsProcessed<=25292)
			{	
				//Adding data to training collection
				trainingCollection.insert(document);
				numberOfBusinessDocumentsProcessed++;
				
				
			}
			else
			{
				//Adding data to test collection
				testCollection.insert(document);
				numberOfBusinessDocumentsProcessed++;
				
			}
			
		}
	}
	}
	
	

