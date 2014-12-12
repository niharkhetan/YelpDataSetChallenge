package task1;
/**
 * @author Bipra De
 * This code assigns programmatically computed categories to the test data
 * Date : December 4th, 2014
 */


	
	import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
	public class AssignCategoriesToTestData {
		private MongoClient mongoClient;
		private DB db;
		private DBCollection featureCollection;
		private DBCollection outputCollection;
		private String indexDirPath;
		private Similarity rankingAlgorithm;
		
		AssignCategoriesToTestData(MongoClient mongoClient,DB db,DBCollection featureCollection,DBCollection outputCollection,String indexDirPath,Similarity rankingAlgorithm)
		{
			this.mongoClient=mongoClient;
			this.db=db;
			this.featureCollection=featureCollection;
			this.outputCollection=outputCollection;
			this.indexDirPath=indexDirPath;
			this.rankingAlgorithm=rankingAlgorithm;
		}
		
		public static void main(String[] args) throws ParseException, IOException
		{
			//Defining required parameters to create an object of the "AssignCategoriesToTestData" class
			
			// Index Directory Path for the Test Data
			String indexDirPath="/Users/biprade/Documents/ILS Z534 Info Retrieval/Final_Project/IndexDirTest/";
			
			//Prepare connection to MongoDB with the YELP database
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			DB db = mongoClient.getDB( "yelp" );
			
			//"feature_set" collection stores the unique categories from the training set and their corresponding top N features
			DBCollection featureCollection = db.getCollection("feature_set");
			
			//"categories_assigned_from_code" stores the categories assigned programmatically for each business ID in the test collection
			DBCollection outputCollection = db.getCollection("categories_assigned_from_code");
			
			//Ranking algorithm to be used [BM25/Dirichlet]
			Similarity rankingAlgorithm=new BM25Similarity();
			
			AssignCategoriesToTestData computeCategories=new AssignCategoriesToTestData(mongoClient,db,featureCollection,outputCollection,indexDirPath,rankingAlgorithm);
			
			//Fetch similar categories from "CategorySimiliarityComparer" class to increase Recall
			CategorySimiliarityComparer groupCategories=new CategorySimiliarityComparer();
			HashMap<String, ArrayList<String>> groupedCategories=groupCategories.returnGroupedCollections();
			
			
			
			//Initializing QueryParser with EnglishAnalyzer on "reviewsandtips" field of lucene index on test data
			Analyzer analyzer = new EnglishAnalyzer();
			QueryParser parser = new QueryParser("reviewsandtips", analyzer);
			
			//Declaring variables necessary for MongoDB query operations
			String queryString;
			DBCursor outputCursor = null;
			DBObject outputResult=null;
			DBObject outputQueryString=null;
			DBObject queryString1=new BasicDBObject();
			DBCursor cursor=featureCollection.find(queryString1);
			DBObject result;
			String category;
			String query;
			
			
			
			//Initialize Index Reader and searcher
			IndexReader reader = DirectoryReader .open(FSDirectory.open(new File(indexDirPath)));
			IndexSearcher searcher = new IndexSearcher(reader);
			searcher.setSimilarity(rankingAlgorithm); 
			
			//Iterating over each category and its top N features in the feature_set collection. Each category's feature is a query against the lucene test index.
			while (cursor.hasNext())
			{
				result=cursor.next();
				category=(String) result.get("category");
				query=(String) result.get("features");
				//Call "assignCategories" method to assign the computed categories to the test data set
				computeCategories.assignCategories(searcher, parser, query,outputCollection,outputCursor,outputResult,outputQueryString,category,10,groupedCategories);
			
				}
			//Close the Index Reader
			reader.close();
			
			
		}

/**
 * This method is used to remove duplicate categories assigned by code that matches with the existing categories in the data set for a business ID
 * @param categories
 * @return filtered categories list without duplicates
 */

		private  String removeDuplicates(String categories) {
			List<String> list = Arrays.asList(categories.split(","));
			List<String> uniqueList = new ArrayList<>();
			List<String> dupsRemoved = new ArrayList<>();
			String dupsRemovedStr = "";
			for (String eachItem : list){
				uniqueList.add(eachItem.trim());			
			}
			for (String eachItem : uniqueList){
				if (!dupsRemoved.contains(eachItem)){
					dupsRemoved.add(eachItem);
				}			
			}
			for (String eachitem : dupsRemoved){
				dupsRemovedStr += eachitem + ", ";
			}
	        return dupsRemovedStr.substring(0, dupsRemovedStr.length()-2);
		}
		
/***
 * This method is used to assign programmatically computed categories to the test data 
 * @param searcher
 * @param parser
 * @param queryString
 * @param outputCollection
 * @param outputCursor
 * @param outputResult
 * @param outputQueryString
 * @param category
 * @param numberOfRankedResults
 * @throws ParseException
 * @throws IOException
 */
		private  void assignCategories(IndexSearcher searcher,
				QueryParser parser, String queryString,DBCollection outputCollection,DBCursor outputCursor,DBObject outputResult,DBObject outputQueryString,String category,int numberOfRankedResults,HashMap<String, ArrayList<String>> groupedCategories) throws ParseException,
				IOException {
			
			DBObject insertString;
			DBObject updateString;
			DBObject searchString;
			DBObject updateCategory;
			String categories;
			
			//Check if the feature set is not empty or null
			if ((!queryString.equals("")) && (null!=queryString))
			{
					//Query against the lucene test data index to get the top N results for a given feature set as query
					Query query = parser.parse(queryString);
					TopScoreDocCollector collector = TopScoreDocCollector.create(numberOfRankedResults, true);
					searcher.search(query, collector);
					ScoreDoc[] docs = collector.topDocs().scoreDocs;
					
					//Fetch similar categories for a given category to increase Recall
					if (groupedCategories.keySet().contains(category) && groupedCategories.get(category).size()>0 )
						{
						category+=","+ (groupedCategories.get(category).toString().replace("[","").replace("]", "").replace(", ",","));
						}
					//Assign the computed categories to the business IDs in the "categories_assigned_from_code" collection
					for (int i = 0; i < docs.length; i++) 
					{
						Document doc = searcher.doc(docs[i].doc);
						outputQueryString=new BasicDBObject("business_id",doc.get("business_id"));
						outputCursor=outputCollection.find(outputQueryString);
						
						//If the category field of the business is empty do an insert operation
						if (!outputCursor.hasNext())
						{
							insertString=new BasicDBObject("business_id",doc.get("business_id")).append("categories",category);
							outputCollection.insert(insertString);
						}
						//If the category field of the business is not empty do an update operation i.e. append the new categories to the existing categories
						else
						{
							outputResult=outputCursor.next();
							categories=(String) outputResult.get("categories")+","+category;
							
							//Check and remove the categories assigned by code that are already present in the database for the given business ID
							categories=removeDuplicates(categories);
		
							searchString=new BasicDBObject("business_id",doc.get("business_id"));
							updateCategory=new BasicDBObject("categories",categories);
							updateString=new BasicDBObject("$set",updateCategory);
							outputCollection.update(searchString, updateString);
						}
						
					}
			
		    }
		}
	}


