package task2;
/*
 * indexCompFeatureSetExtractorForEachBusiness.java
 * @author: Bipra De, Nihar Khetan, Satvik Shetty, Anand Saurabh
 * 
 * @created on: 26th November, 2014
 * 
 * This Class reads through lucene index of full dataset
 * It iterates over each  business and its review and tips, finds tf-idf to get top n features.
 * n is a parameter here which can vary
 * Creates a mongoDB collection 'feature_set' which contains extracted features of businesses defined by a filter:
 * 		filter: as in this task only with respect to restaurants so specific categories has to be defined for businesses for 
 * 				which feature set need to be extracted. For eg: restaurant | food | mexican | chinese | thai 
 * 
 * multiple filters are applied for feature set pre-processing before adding it to feature_set dictionary for businesses * 
 * 
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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

public class FeatureSetExtractorForEachBusiness {
	/**
	 * @param args
	 * @throws IOException 
	 * Main function: 	Calls indexWriterFunction
	 * 					Calls indexReaderFunctoin
	 * Creates the index for given corpus and then reads the indexed corpus
	 * 
	 * INDEX path: 
	 * CHANGE indexPath = "<PATH ON YOUR MACHINE WHERE YOU WANT INDEX FILES IS STRORED>"	
	 */
	
	public static void main(String[] args) throws IOException {
		
		String indexPath = "/Users/biprade/Documents/ILS Z534 Info Retrieval/Final_Project/IndexDirTestTask2/";		
		indexReaderFunction(indexPath);			
	}
	
	/*
	 * @param String indexPath
	 * @throws IOException
	 *  
	 * Reads Indexed directory and iterates over all businesses and their review and tips
	 * 
	 * */
	private static void indexReaderFunction(String indexPath) throws IOException{
		IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));

		Terms vocabulary = MultiFields.getTerms(reader, "reviewsandtips");

		Double TfIdfScore;
		IndexSearcher searcher = new IndexSearcher(reader);
		
		// connecting to mongoDB on local port
		MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		DB db = mongoClient.getDB( "yelp" );
		DBCollection collection = db.getCollection("feature_set_for_restaurants");
    	DBObject insertString;
    	
    	// iterating over index to get feature set for each business
		Integer totalDocs = reader.maxDoc();
		for(int i=0; i < totalDocs ; i++){
			HashMap<String,Double> termTfIdfScore = new HashMap<>();
			final Terms terms = reader.getTermVector(i, "reviewsandtips");
			Document indexDoc = searcher.doc(i);
			if (indexDoc.get("categories")!=null){
				
				// To add more restaurants simply add category names to regular expression below
				if ( (indexDoc.get("categories")).matches(".*\\b(chinese|Chinese|Food|food|Mexican|mexican|Restaurants|American (Traditional)|Fast Food|Italian|Steakhouses|Hot Dogs|American (New)|Caribbean)\\b.*")){
					if (terms != null && terms.size() > 0) {
					    TermsEnum termsEnum = terms.iterator(null); // access the terms for this field
					    BytesRef term = null;
					    while ((term = termsEnum.next()) != null) {// explore the terms for this field
					        DocsEnum docsEnum = termsEnum.docs(null, null); // enumerate through documents, in this case only one
					        int docIdEnum;
					        while ((docIdEnum = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
						          TfIdfScore = ((docsEnum.freq()))*(Math.log10(totalDocs/reader.docFreq(new Term("reviewsandtips", term.utf8ToString()))));
						          //filters applied below for pre processing
						          // filter1 : searches for numbers
						          // filter2 : searches for website names
						          // filter3 : searches for words ending with 's
						          // filter4 : searches for words with dot. for eg: tiny.world
						          // filter5 : searches for one or two letter words specifically  
						          // filter6 : searches for words like aaaaaaa lolololo mapmapmapmap which has repeating sequences in it
						          // filter7 : filters terms in category or business (check function comments for more detail)
						          // filter8 : lemmatizer for business name to feature sets
						          // filter9 : lemmatizer for business name to feature sets
						          if ((term.utf8ToString().matches(".*\\d.*")) || 							
						        		  (term.utf8ToString().matches(".*.*\\b(www.|.com)\\b.*.*")) ||
						        		  (term.utf8ToString().matches(".*'s.*")) ||
						        		  (term.utf8ToString().matches(".*\\..*")) ||
						        		  (term.utf8ToString().matches("^[a-zA-Z]{1,3}$")) ||
						        		  filterRepeatingChars(term.utf8ToString()) ||
						        		  isTermInCategoryOrBusiness(indexDoc.get("categories"),indexDoc.get("business_name"),term.utf8ToString()) ||
						        		  sCharlemmatizer(indexDoc.get("business_name"),term.utf8ToString()) ||
						        		  sCharlemmatizer1(indexDoc.get("business_name"),term.utf8ToString())){
						        	  	  // do nothing
						          }
						          else{	
						        	  // filter to check if feature set has lemmatized form of words already added to feature set
						        	  if( !featureSetFilterOnCharS(termTfIdfScore, term.utf8ToString())){
						        		  termTfIdfScore.put(term.utf8ToString(), TfIdfScore);
						        	  }				        	  
						        }		          
						    }
					    }
					}
							
					Map<String, Double> sortedTfIdfScore = sortByComparator(termTfIdfScore);
					Integer count = 0;
					String featureSet = "";
					for (Map.Entry<String, Double> entity: sortedTfIdfScore.entrySet()){
						if (count != 10){						
							featureSet = featureSet + " " + entity.getKey();
							count = count + 1;
						}
						else{
							break;
						}					
					}								
					insertString=new BasicDBObject("business_id",indexDoc.get("business_id")).append("features",featureSet);
					collection.insert(insertString);
					System.out.println("Feature Set generated for :" + indexDoc.get("business_id"));
					System.out.println("Features: " + featureSet);
				}
			}
		}		
		reader.close();
	}
	
	/**
	 * @param : Map<String, Double> unsortMap : a Map which has to be sorted on keys
	 * @return: Map<String, Double> : sortedMap 
	 * 
	 * Function sorts a map on keys and returns the sorted map
	 * 
	 */	
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
	
	/**
	 * @param : String stringToMatch: checkString 
	 * @return: boolean
	 * 
	 * Function checks if string has words like: aaaaaa, bobobobobo 
	 * which have 2 or more repeating characters or bunch of characters
	 * 
	 */
	
	private static boolean filterRepeatingChars(String stringToMatch){
		Pattern p = Pattern.compile("(\\w\\w)\\1+");
		Matcher m = p.matcher(stringToMatch);
		if (m.find())
		{
		    return true;
		}
		else {
			return false;		
		}
	}
	
	/**
	 * @param : String Category, String Business, String term
	 * @return: boolean
	 * 
	 * Function checks if term is present in Category name or Business name
	 * it also check is lemmatized form of word exist or not 
	 * for eg: term : cullar; then it check for both cullar's and cullars
	 */
	
	public static boolean isTermInCategoryOrBusiness(String Category, String Business, String term){
		if (Category==null) Category = "";
		if (Business==null) Business = "";
		if (Category.toLowerCase().matches(".*\\b"+term.toLowerCase()+"\\b.*") || 
				Business.toLowerCase().matches(".*\\b"+term.toLowerCase()+"\\b.*") || 
				term.toLowerCase().matches(".*\\b(food|restaurant)\\b.*") ||
				Business.toLowerCase().matches(".*\\b"+term.toLowerCase()+"(s|'s)\\b.*")){
			return true;
		}
		
		return false;		
	}
	
	/**
	 * @param : String Business, String term
	 * @return: boolean
	 * 
	 * Function checks is term, is present in business
	 * it also check is lemmatized form of wrod exist or not 
	 * for eg: term : cullar; then it check for both cullar's and cullars
	 * 
	 */
	
	public static boolean sCharlemmatizer(String Business, String term){
		if (Business==null) return false;
		String[] businessTerms = Business.split(" ");
		for (String eachTerm : businessTerms){
			if (term.toLowerCase().matches(".*\\b"+eachTerm.toLowerCase()+"(s|'s)\\b.*")){
				return true;
			}
		}		
		return false;		
	}
	
	/**
	 * @param : String Business, String term
	 * @return: boolean
	 * 
	 * Function checks if inverse lemmatized form of term, is present in business
	 * for eg: term: cullars then it checks for cullar in business name
	 * 
	 */
	
	public static boolean sCharlemmatizer1(String Business, String term){
		String[] businessTerms = Business.split(" ");
		for (String eachTerm : businessTerms){			
			if (eachTerm.length()>2){
				if (eachTerm.toLowerCase().charAt(eachTerm.length()-1) == 's' && (char)eachTerm.toLowerCase().charAt(eachTerm.length()-2) == 39){
					eachTerm = eachTerm.substring(0, eachTerm.length()-2) + "s";
					
					if (term.toLowerCase().matches(".*\\b"+eachTerm.toLowerCase()+"\\b.*")){
						return true;
					}
				}			
			}
		}		
		return false;		
	}
	
	/**
	 * @param : HashMap<String, Double> temTfIdfScore, String newFeature
	 * @return: boolean
	 * 
	 * checks if lemma of the term exists in feature set hashmap which is already created	
	 * 
	 */
	
	public static Boolean featureSetFilterOnCharS(HashMap<String, Double> temTfIdfScore, String newFeature){
		Set<String> TfIdfKeySet = temTfIdfScore.keySet(); 
		Boolean doesItMatch = false;
		for (String eachItem : TfIdfKeySet){			
			if (eachItem.length() > newFeature.length()){
				if(str1GreaterThanStr2(newFeature, eachItem) == true){
					doesItMatch = doesItMatch || true ;
				} 
			}
			else if (eachItem.length() < newFeature.length()){
				if(str1GreaterThanStr2(eachItem, newFeature) == true){
					doesItMatch = doesItMatch || true ;
				}
			}
		}
		return doesItMatch;
	}
	
	/**
	 * @param : String stringToMatch: String smallStr, String bigStr
	 * @return : boolean
	 * Function checks if two strings entered as arguements only differ on their lemma that is 's'
	 * 
	 */
	
	public static Boolean str1GreaterThanStr2(String smallStr, String bigStr ){
		Integer Flag = 0;
		smallStr = smallStr.toLowerCase();
		bigStr = bigStr.toLowerCase();
		for (int i = 0 ; i < smallStr.length()-1 ; i++){
			if (smallStr.charAt(i) == bigStr.charAt(i)){
				continue;
			}
			else {
				Flag = 1;
				break;
			}
		}
		if (Flag == 0){			
			String str3 = smallStr + 's';
			if (str3.equals(bigStr)) return true;
			else return false;
		}
		return false;
	}	
}
	
