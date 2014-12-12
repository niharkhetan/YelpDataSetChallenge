package task2;
/**
 * @author Bipra De
 * This code creates lucene index for the complete YELP data set for Task 2
 * Date : December 1st, 2014
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class GenerateTask2Index {
	private  MongoClient mongoClient;
	private  DB db;

	GenerateTask2Index(MongoClient mongoClient,DB db) throws UnknownHostException
	 {
			this.mongoClient = mongoClient;
			this.db = db;
	 }
	public static void main(String[] args) throws CorruptIndexException,
			LockObtainFailedException, IOException
			 {

						// Initializing required parameters for the GenerateTask2Index class
						File indexDir=new File("/Users/biprade/Documents/ILS Z534 Info Retrieval/Final_Project/IndexDirTask2/");
						MongoClient mongoClient=new MongoClient( "localhost" , 27017 );
						DB db=mongoClient.getDB( "yelp" );
						
						//Create Lucene Index on the complete data set
						GenerateTask2Index indexGenerator=new GenerateTask2Index(mongoClient,db);
						indexGenerator.createIndex(indexDir,new StandardAnalyzer());	
			
	}

	
	



/**
 * Method to create lucene index for the entire YELP data set
 * @param index directory path, analyzer
 * @throws IOException
 * @throws FileNotFoundException
 */
 
	private  void  createIndex(File indexDir,Analyzer analyzer) throws IOException,
			FileNotFoundException {
				
		//Creating and Initializing IndexWriter
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_4_10_1,analyzer);
		indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		Directory fileSystemDirectory = FSDirectory.open(indexDir);
		IndexWriter writer = new IndexWriter(fileSystemDirectory, indexWriterConfig);
		
		//MongoDB collections for training and test set
		
		DBCollection testCollection = db.getCollection("test_set");
		DBCollection trainingCollection = db.getCollection("training_set");
		
		//Declaring variables for query operations on MongoDB
		DBObject queryString=new BasicDBObject();
		DBCursor testCursor = testCollection.find(queryString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		DBCursor trainingCursor = trainingCollection.find(queryString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		DBObject testResult;
		DBObject trainingResult;
		
		//Declaring variables to store field values from the query result
		String businessID;
		List categories;
		String businessName;
		List reviews;
		List reviewStars;
		List tips;
		List tipsLikes;
		
		
		FieldType type = new FieldType();
		type.setIndexed(true);
		type.setStored(true);
		type.setStoreTermVectors(true);
		
		try{	
				//Iterating over training collection
				while (trainingCursor.hasNext())
				{
					trainingResult=trainingCursor.next();
					//Creating a lucene document
					Document luceneDoc = new Document();
					
					//Adding fields to the lucene document
					businessID=trainingResult.get("business_id").toString();
					luceneDoc.add(new StringField("business_id",businessID, Store.YES));
					
					businessName=trainingResult.get("name").toString();
					luceneDoc.add(new StringField("business_name",businessName, Store.YES));
					
					categories=(List)trainingResult.get("categories");
					for (Object category:categories)
					{
						luceneDoc.add(new StringField("categories",category.toString(), Store.YES));
					}
					
					//Preparing a concatenated string of reviews and tips
					String reviewsAndTips="";
					reviews=(List)trainingResult.get("reviews");
					for (Object review:reviews)
					{
						reviewsAndTips+=review.toString();
					}
		
					tips=(List)trainingResult.get("tips");
					for (Object tip:tips)
					{
						reviewsAndTips+=tip.toString();
					}
		
					Field field = new Field("reviewsandtips", reviewsAndTips, type);
					luceneDoc.add(field);
					
					// Write the lucene document to the index
					writer.addDocument(luceneDoc);
					
					
				}
				
				//Iterating over test collection
				while (testCursor.hasNext())
				{
					testResult=testCursor.next();
					//Creating a lucene document
					Document luceneDoc = new Document();
					
					//Adding fields to the lucene document
					businessID=testResult.get("business_id").toString();
					luceneDoc.add(new StringField("business_id",businessID, Store.YES));
					
					businessName=testResult.get("name").toString();
					luceneDoc.add(new StringField("business_name",businessName, Store.YES));
					
		
					
					categories=(List)testResult.get("categories");
					for (Object category:categories)
						luceneDoc.add(new StringField("categories",category.toString(), Store.YES));
					
					//Preparing a concatenated string of reviews and tips
					String reviewsAndTips="";
					reviews=(List)testResult.get("reviews");
					for (Object review:reviews)
					{
						reviewsAndTips+=review.toString();
					}
		
					
					tips=(List)testResult.get("tips");
					for (Object tip:tips)
					{
						reviewsAndTips+=tip.toString();
					}
		
					Field field = new Field("reviewsandtips", reviewsAndTips, type);
					luceneDoc.add(field);	
					
					// Write the lucene document to the index
					writer.addDocument(luceneDoc);
					
					
				}
				
		 }
			finally
				{	testCursor.close();
					trainingCursor.close();
				 	writer.forceMerge(1);
					writer.commit();
					writer.close();
				}
		}
		
	}

