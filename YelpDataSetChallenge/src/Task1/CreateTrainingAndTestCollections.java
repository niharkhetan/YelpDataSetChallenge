package Task1;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.fst.NoOutputs;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class CreateTrainingAndTestCollections {

	public static void main(String args[]) throws UnknownHostException
	{
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelp" );
		//Defining the training and test set
//		DBCollection trainingCollection = db.getCollection("training_set");
//		DBCollection testCollection = db.getCollection("test_set");
		
		DBCollection task2Collection = db.getCollection("complete_data_set");
		
		//Using the exisitng collections to prepare the training and test set
		DBCollection businessCollection = db.getCollection("business");
		DBCollection reviewsCollection = db.getCollection("reviews");
		DBCollection tipsCollection = db.getCollection("tips");
		
		DBObject projectionString=new BasicDBObject("_id",0).append("business_id",1).append("categories",1).append("name",1);
		DBObject reviewsProjectionString=new BasicDBObject("_id",0).append("stars",1).append("text",1);
		DBObject tipsProjectionString=new BasicDBObject("_id",0).append("likes",1).append("text",1);
		DBCursor businessCursor=businessCollection.find(new BasicDBObject(),projectionString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		DBCursor reviewsCursor;
		DBObject reviewQueryString;
		DBCursor tipsCursor;
		DBObject tipsQueryString;
		int numberOfBusinessDocumentsProcessed=0;
		String businessName;
		List categories;
		String businessID;
		DBObject businessObject;
		DBObject reviewsObject;
		DBObject tipsObject = null;
		DBCursor trainingCursor;
		DBCursor testCursor;
		DBCursor task2Cursor;
		DBObject queryString;
		while (businessCursor.hasNext())
		{
			businessObject=businessCursor.next();
			queryString=new BasicDBObject("business_id",(String)businessObject.get("business_id"));
			//trainingCursor=trainingCollection.find(queryString);
			//testCursor=testCollection.find(queryString);
//			if((!trainingCursor.hasNext())&&(!testCursor.hasNext()))
//			{
//			businessName=(String) businessObject.get("name");
//			categories=(List)businessObject.get("categories");
//			businessID=(String)businessObject.get("business_id");
			List reviews = new ArrayList();
			List reviewStars = new ArrayList();
			List tips = new ArrayList();
			List tipsLikes = new ArrayList();
			//Added business collection data to the BSON Object
			DBObject document=new BasicDBObject(businessObject.toMap());
			
			//Adding reviews  data to the BSON Object
			reviewQueryString=new BasicDBObject("business_id",(String)businessObject.get("business_id"));
			reviewsCursor=reviewsCollection.find(reviewQueryString,reviewsProjectionString);
			while (reviewsCursor.hasNext())
			{
				reviewsObject=reviewsCursor.next();
				reviews.add((String) reviewsObject.get("text"));
				if (null==reviewsObject.get("stars"))
					reviewStars.add(0);
				else
					reviewStars.add((int)reviewsObject.get("stars"));
				
			}
			
			document.put("reviews",reviews );
			document.put("reviewStars", reviewStars);
			
			//Adding tips data to the BSON Object
			tipsQueryString=new BasicDBObject("business_id",(String)businessObject.get("business_id"));
			tipsCursor=tipsCollection.find(tipsQueryString,tipsProjectionString);
			while (tipsCursor.hasNext())
			{
				tipsObject=tipsCursor.next();
				tips.add((String) tipsObject.get("text"));
				if(null==tipsObject.get("likes"))
					tipsLikes.add(0);
				else
					tipsLikes.add((int)tipsObject.get("likes"));
				
			}
			
			document.put("tips",tips );
			document.put("tipsLikes", tipsLikes);
			
			
//			if (numberOfBusinessDocumentsProcessed<=25292)
//			{	
//				
//				trainingCollection.insert(document);
//				numberOfBusinessDocumentsProcessed++;
//				System.out.println("Training "+ businessObject.get("business_id"));
//				
//			}
//			else
//			{
				
				task2Collection.insert(document);
				numberOfBusinessDocumentsProcessed++;
				System.out.println(numberOfBusinessDocumentsProcessed);
			//}
			
		//}
		
		}
	}
	
	
}
