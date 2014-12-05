package task1;
/**
 * @author Bipra De
 * This code indexes the documents in the corpus using StandardAnalyzer and outputs the desired results
 * Date : October 4th, 2014
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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

	//private static String docDirPath; //= "/Users/biprade/Documents/ILS Z534 Info Retrieval/Assignment 1/Corpus/";
	private static String indexDirPath; //= "/Users/biprade/Documents/ILS Z534 Info Retrieval/Final_Project/IndexDir/";
	//private static File docDir;
	private static File indexDir;
	private static File indexDirTest;
	private static MongoClient mongoClient;
	private static DB db;
	public static void main(String[] args) throws CorruptIndexException,
			LockObtainFailedException, IOException
			 {

			indexDir=new File("/Users/biprade/Documents/ILS Z534 Info Retrieval/Final_Project/IndexDirTraining/");
			indexDirTest=new File("/Users/biprade/Documents/ILS Z534 Info Retrieval/Final_Project/IndexDirTest/");
			//Calling the index creation method 
			//indexCreationForTrainingSet(new EnglishAnalyzer());
			indexCreationForTestSet(new EnglishAnalyzer());

			
	}

	
	
/**
 * Method to Index the training documents in a corpus
 * @param analyzer
 * @throws IOException
 * @throws FileNotFoundException
 */
 
	private static void  indexCreationForTrainingSet(Analyzer analyzer) throws IOException,
			FileNotFoundException {
				
		//Creating and Initializing IndexWriter
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_4_10_1,analyzer);
		indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		Directory fileSystemDirectory = FSDirectory.open(indexDir);
		IndexWriter writer = new IndexWriter(fileSystemDirectory, indexWriterConfig);
		
		//MongoDB connection
		mongoClient = new MongoClient( "localhost" , 27017 );
		db = mongoClient.getDB( "yelp" );
		DBCollection collection = db.getCollection("training_set");
		DBObject projectionString=new BasicDBObject("_id",0).append("reviews",1).append("tips",1);
		DBObject filterCategoriesQueryString;
		List<String> categoriesList = (List<String>)collection.distinct("categories");
		categoriesList.removeAll(Collections.singleton(null));
		//System.out.println(categoriesList);
		DBCursor cursor = null;
		DBObject result;
		int count=0;
		String reviewsAndTips="";
try{	
		for (String category:categoriesList)
		{
			
			//String tips="";
			
				//System.out.println("BINGO");
				filterCategoriesQueryString=new BasicDBObject("categories",category);
				cursor=collection.find(filterCategoriesQueryString,projectionString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
				Document luceneDoc = new Document();
				luceneDoc.add(new StringField("category",category, Store.YES));
				while(cursor.hasNext())
				{
					result=cursor.next();
					reviewsAndTips=result.get("reviews").toString()+result.get("tips").toString();
					
					
				
				reviewsAndTips=reviewsAndTips.replace("[", "").replace("]","");
				//System.out.println(reviewsAndTips);
				
				
				
				if (reviewsAndTips != ""){
					
						FieldType type = new FieldType();
						type.setIndexed(true);
						type.setStored(true);
						type.setStoreTermVectors(true);
						Field field = new Field("reviewsandtips", reviewsAndTips, type);
						luceneDoc.add(field);								
						
					
				
				
				
				}
			}
				//continue;
				writer.addDocument(luceneDoc);
			//System.out.println(category);

		System.out.println(++count);
		System.out.println(category);
		}
		
		}
		finally
			{cursor.close();
			
		 	writer.forceMerge(1);
			writer.commit();
			writer.close();
			}
	}
		
	


/**
 * Method to Index the test documents in a corpus
 * @param analyzer
 * @throws IOException
 * @throws FileNotFoundException
 */
 
	private static void  indexCreationForTestSet(Analyzer analyzer) throws IOException,
			FileNotFoundException {
				
		//Creating and Initializing IndexWriter
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_4_10_1,analyzer);
		indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		Directory fileSystemDirectory = FSDirectory.open(indexDirTest);
		IndexWriter writer = new IndexWriter(fileSystemDirectory, indexWriterConfig);
		
		//MongoDB connection
		mongoClient = new MongoClient( "localhost" , 27017 );
		db = mongoClient.getDB( "yelp" );
		DBCollection collection = db.getCollection("test_set");
		DBObject projectionString=new BasicDBObject("_id",0);
		DBObject queryString=new BasicDBObject();
//		List<String> categoriesList = (List<String>)collection.distinct("categories");
//		categoriesList.removeAll(Collections.singleton(null));
		//System.out.println(categoriesList);
		DBCursor cursor = collection.find(queryString,projectionString).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		
		DBObject result;
		String businessID;
		//String categories;
		List categories;
		String businessName;
		List reviews;
		List reviewStars;
		List tips;
		List tipsLikes;
		int count=0;
		
try{	
		while (cursor.hasNext())
		{
			result=cursor.next();
			Document luceneDoc = new Document();
			String reviewsAndTips="";
			businessID=result.get("business_id").toString();
			luceneDoc.add(new StringField("business_id",businessID, Store.YES));
			
			businessName=result.get("name").toString();
			luceneDoc.add(new StringField("business_name",businessName, Store.YES));
			
//			categories=result.get("categories").toString().replaceAll("[\\[\\]\"]", "");
			
			categories=(List)result.get("categories");
			for (Object category:categories)
				luceneDoc.add(new StringField("categories",category.toString(), Store.YES));
			
			reviews=(List)result.get("reviews");
			for (Object review:reviews)
				reviewsAndTips+=review.toString();
			
//			reviewStars=(List)result.get("reviewStars");
//			for (Object stars:reviewStars)
//				luceneDoc.add(new StringField("review stars",stars.toString(), Store.YES));
			
			tips=(List)result.get("tips");
			for (Object tip:tips)
				reviewsAndTips+=tip.toString();
			
//			tipsLikes=(List)result.get("tipsLikes");
//			for (Object likes:tipsLikes)
//				luceneDoc.add(new StringField("tips likes",likes.toString(), Store.YES));
			luceneDoc.add(new TextField("reviewsandtips",reviewsAndTips, Store.YES));
			writer.addDocument(luceneDoc);
			System.out.println(++count);
			
		}
		
		}
		finally
			{cursor.close();
			
		 	writer.forceMerge(1);
			writer.commit();
			writer.close();
			}
		}
		
	}

