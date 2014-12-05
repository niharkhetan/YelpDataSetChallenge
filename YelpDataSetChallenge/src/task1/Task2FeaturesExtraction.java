package task1;
/*
 * indexComparison.java
 * @author: Nihar Khetan
 * @username: nkhetan
 * @created on: 3rd October, 2014
 * 
 * This program indexes the given corpus AP89 using StandardAnalyzer then reads the index
 * */

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class Task2FeaturesExtraction {

	/**
	 * @param args
	 * @throws IOException 
	 * Main function: 	Calls indexWriterFunction
	 * 					Calls indexReaderFunctoin
	 * Creates the index for given corpus and then reads the indexed corpus
	 * 
	 * INDEX path: 
	 * CHANGE indexPath = "<PATH ON YOUR MACHINE WHERE YOU WANT INDEX FILES TO BE STRORED>"
	 * 
	 * CORPUS path:
	 * CHANGE baseFolder = "<PATH OF CORPUS ON YOUR MACHINE>"
	 */
	String protocol = "http";
	String baseurl = "en.wikipedia.org";
	//String service url = "/w/api.php?action=query&list=search&srsearch=";
	String outputformat = "format=json";
	public static void main(String[] args) throws IOException {
		
		String indexPath = "/Users/biprade/Documents/ILS Z534 Info Retrieval/Final_Project/IndexDirTest/";
		System.out.println("<<<<<< :: Indexing.......................... ");
		System.out.println(" ");
		
		indexReaderFunction(indexPath);	
		
	}

	
	/*
	 * @param String indexPath
	 * @throws IOException
	 *  
	 * Reads Indexed directory
	 * 
	 * */
	private static void indexReaderFunction(String indexPath) throws IOException{
		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
		
		//Print the total number of documents in the corpus
		System.out.println("Total number of documents in the corpus: "+reader.maxDoc());
		
		//Print the number of documents containing the term "new" in <field>TEXT</field>. 
		//System.out.println("Number of documents containing the term \"new\" for field \"TEXT\": "+reader.docFreq(new Term("reviewsandtips", "new")));
		
		//Print the total number of occurrences of the term "new" across all documents for <field>TEXT</field>.
		//System.out.println("Number of occurences of \"new\" in the field \"TEXT\": "+reader.totalTermFreq(new Term("reviewsandtips","new")));
		Terms vocabulary = MultiFields.getTerms(reader, "reviewsandtips");
		
		//Print the size of the vocabulary for <field>content</field>, only available per-segment.
		System.out.println("Size of the vocabulary for this field: "+vocabulary.size());
		
		//Print the total number of documents that have at least one term for <field>TEXT</field>
		System.out.println("Number of documents that have at least one term for this field: "+vocabulary.getDocCount());
		
		//Print the total number of tokens for <field>TEXT</field>
		System.out.println("Number of tokens for this field: "+vocabulary.getSumTotalTermFreq());
		
		//Print the total number of postings for <field>TEXT</field>
		System.out.println("Number of postings for this field: 	"+vocabulary.getSumDocFreq());
							//store docIds as key and scores as values
		HashMap<String,String> categoryFeatureSet = new HashMap<>();
		Double TfIdfScore;
		IndexSearcher searcher = new IndexSearcher(reader);
		
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelp" );
		DBCollection collection = db.getCollection("feature_set");
    	DBObject insertString;
		Integer totalDocs = reader.maxDoc();
		for(int i=0; i < totalDocs ; i++){
			HashMap<String,Double> termTfIdfScore = new HashMap<>();
			final Terms terms = reader.getTermVector(i, "reviewsandtips");
			if (terms != null && terms.size() > 0) {
			    TermsEnum termsEnum = terms.iterator(null); // access the terms for this field
			    BytesRef term = null;
			    while ((term = termsEnum.next()) != null) {// explore the terms for this field
			        DocsEnum docsEnum = termsEnum.docs(null, null); // enumerate through documents, in this case only one
			        int docIdEnum;
			        while ((docIdEnum = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
			          TfIdfScore = (docsEnum.freq()^2)*(Math.log10(totalDocs/reader.docFreq(new Term("reviewsandtips", term.utf8ToString()))));
			          if (!(term.utf8ToString().matches(".*\\d.*"))){
			        	  termTfIdfScore.put(term.utf8ToString(), TfIdfScore);
			          }			          
			        }
			    }
			}
					
			Map<String, Double> sortedTfIdfScore = sortByComparator(termTfIdfScore);
			Integer count = 0;
			String featureSet = "";
			for (Map.Entry<String, Double> entity: sortedTfIdfScore.entrySet()){
				if (count != 10){											
					//System.out.println(entity.getKey()+ "\t:\t" + entity.getValue());	
					featureSet = featureSet + " " + entity.getKey();
					count = count + 1;
				}
				else{
					break;
				}					
			}
			Document indexDoc = searcher.doc(i);
			categoryFeatureSet.put(indexDoc.get("category"), featureSet);
			insertString=new BasicDBObject("category",indexDoc.get("category")).append("features",featureSet);
			collection.insert(insertString);
			System.out.println("Feature Set generated for :" + indexDoc.get("category"));
			System.out.println("Features: " + featureSet);
		}
		//System.out.println(categoryFeatureSet);
		
		reader.close();
	}
	
	private static Map<String, Double> sortByComparator(Map<String, Double> unsortMap) {
		 
		// Convert Map to List
		List<Map.Entry<String, Double>> list = 
			new LinkedList<Map.Entry<String, Double>>(unsortMap.entrySet());
 
		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});
 
		// Convert sorted map back to a Map
		Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
		for (Iterator<Map.Entry<String, Double>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Double> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}


	
}
	
