package task1;
/**
 * @author Bipra De, Nihar Khetan, Satvik Shetty, Anand Saurabh
 * This code creates indexes on training and test data for TASK 1
 * Date : November 30th, 2014
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
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

public class GenerateIndex {
	
	//private  File indexDirTest;
	private  MongoClient mongoClient;
	private  DB db;
	
	GenerateIndex(MongoClient mongoClient,DB db) throws UnknownHostException
	 {
			this.mongoClient = mongoClient;
			this.db = db;
	 }
	public static void main(String[] args) throws CorruptIndexException,
			LockObtainFailedException, IOException
	 {
			
			//Prepare connection to MongoDB with the YELP database
			MongoClient mongoClient=new MongoClient( "localhost" , 27017 );
			DB db=mongoClient.getDB( "yelp" );
			
			//Create index on Training data set
			//Path to create the lucene training index directory
			File indexDir=new File("/Users/biprade/Documents/ILS Z534 Info Retrieval/Final_Project/IndexDirTraining/");
			GenerateIndex indexGenerator=new GenerateIndex(mongoClient,db);
			indexGenerator.createTrainingIndex(indexDir,new EnglishAnalyzer());
			
			//Create index on Test data set
			//Path to create the lucene training index directory
			indexDir=new File("/Users/biprade/Documents/ILS Z534 Info Retrieval/Final_Project/IndexDirTest/");
			indexGenerator.createTestIndex(indexDir,new EnglishAnalyzer());

			
	}

/**
 * Method to create index for training data set
 * @param index directory path, analyzer
 * @throws IOException
 * @throws FileNotFoundException
 */
 
	private void  createTrainingIndex(File indexDir,Analyzer analyzer) throws IOException,
			FileNotFoundException {
				
		//Creating and Initializing IndexWriter
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_4_10_1,analyzer);
		indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		Directory fileSystemDirectory = FSDirectory.open(indexDir);
		IndexWriter writer = new IndexWriter(fileSystemDirectory, indexWriterConfig);
		
		//MongoDB training collection name
		DBCollection collection = db.getCollection("training_set");
		
		//Preparing Query to fetch all the reviews and tips of each category
		DBObject projectionString=new BasicDBObject("_id",0).append("reviews",1).append("tips",1);
		DBObject fetchCategoriesQueryString;
		DBCursor cursor = null;
		DBObject result;
		
		//Query to fetch all the distinct categories from the training data set and eliminating all null categories
		List<String> categoriesList = (List<String>)collection.distinct("categories");
		categoriesList.removeAll(Collections.singleton(null));		
		
		String reviewsAndTips="";
		try{	
				for (String category:categoriesList)
				{
					
						fetchCategoriesQueryString=new BasicDBObject("categories",category);
						cursor=collection.find(fetchCategoriesQueryString,projectionString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
						
						//Creating a lucene document
						Document luceneDoc = new Document();
						//Adding category field to the lucene document
						luceneDoc.add(new StringField("category",category, Store.YES));
						
						//Iterating over query results i.e. all reviews and tips for a given category
						while(cursor.hasNext())
						{
								//Preparing a concatenated string of all reviews and tips of a category
								result=cursor.next();
								reviewsAndTips=result.get("reviews").toString()+result.get("tips").toString();
								reviewsAndTips=reviewsAndTips.replace("[", "").replace("]","");	
								
								//Adding reviewsandtips field to the lucene document
								if (reviewsAndTips != "")
								{
									
										FieldType type = new FieldType();
										type.setIndexed(true);
										type.setStored(true);
										type.setStoreTermVectors(true);
										Field field = new Field("reviewsandtips", reviewsAndTips, type);
										luceneDoc.add(field);								
								
								}
						}
						//Write the lucene document to the index
						writer.addDocument(luceneDoc);				
				}				
		  }
		
		finally
			{
					cursor.close();
					writer.forceMerge(1);
					writer.commit();
					writer.close();
			}
	}

/**
 * Method to create index for the test data set
 * @param index directory path, analyzer
 * @throws IOException
 * @throws FileNotFoundException
 */
 
	private  void  createTestIndex(File indexDir,Analyzer analyzer) throws IOException,
			FileNotFoundException {
				
		//Creating and Initializing IndexWriter
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_4_10_1,analyzer);
		indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		Directory fileSystemDirectory = FSDirectory.open(indexDir);
		IndexWriter writer = new IndexWriter(fileSystemDirectory, indexWriterConfig);
		
		//MongoDB test collection name
		DBCollection collection = db.getCollection("test_set");
		
		//Preparing Query to fetch all the documents from the test collectio
		DBObject projectionString=new BasicDBObject("_id",0);
		DBObject queryString=new BasicDBObject();
		DBCursor cursor = collection.find(queryString,projectionString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		DBObject result;
		
		//Declaring variables to store field values from the query result
		String businessID;
		List categories;
		String businessName;
		List reviews;
		List reviewStars;
		List tips;
		List tipsLikes;		
		
		try{	
			//Iterating over query results
				while (cursor.hasNext())
				{
					result=cursor.next();
					//Creating a lucene document
					Document luceneDoc = new Document();
					
					//Adding fields to the lucene document
					businessID=result.get("business_id").toString();
					luceneDoc.add(new StringField("business_id",businessID, Store.YES));
					
					businessName=result.get("name").toString();
					luceneDoc.add(new StringField("business_name",businessName, Store.YES));
					
					String reviewsAndTips="";
					categories=(List)result.get("categories");
					for (Object category:categories)
					{
						luceneDoc.add(new StringField("categories",category.toString(), Store.YES));
					}
					
					//Preparing a concatenated string of reviews and tips for each business ID
					reviews=(List)result.get("reviews");
					
					for (Object review:reviews)
					{
						reviewsAndTips+=review.toString();
					}
		
					tips=(List)result.get("tips");
					for (Object tip:tips)
					{
						reviewsAndTips+=tip.toString();
					}
					
					luceneDoc.add(new TextField("reviewsandtips",reviewsAndTips, Store.YES));
					// Write the lucene document to the index
					writer.addDocument(luceneDoc);					
				}		
		  }
		finally
			{		cursor.close();
					writer.forceMerge(1);
					writer.commit();
					writer.close();								
			}
		}		
	}

