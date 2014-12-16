package task2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;

import task1.ParseException;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class MenuFinder {


	 /* This method is used to calculate Sentiment intensity and Sentiment score of Feature List 
	 * @param Review : Reviews + tips of restaurants 
	 * @param dishes : Feature set of particular business 
	 * @throws IOException
	 */
	public HashMap<String, String> getMenu(String Reviews, List<String> Dishes)
			throws IOException {
		
		
		StringBuilder DetectedList = new StringBuilder();
		//Call to findPattern to find text in which dishes occur
		DetectedList = findPattern(Dishes, Reviews);
		FindNouns obj = new FindNouns();
		
		String word = new String();
		String TextString = DetectedList.toString();
				
		//Capitalizing all Nouns in text
		for (int i = 0; i < Dishes.size(); i++) {
			word = Dishes.get(i);
			if (word.length() > 2) {
				String regex = String.format("(?i)(%s)", word);
				TextString = TextString.toString().replaceAll(regex, WordUtils.capitalize(word));	
			}
		}
		
		List<String> NounsDetected = new ArrayList<String>();
		NounsDetected = (obj.getNouns(TextString));
		
		List<String> CommonDishes = new ArrayList<String>();
  		
		//Filtering out on Non Noun words from Dishes list
		for(String Dish : Dishes)
		{
			for(String  Nouns : NounsDetected)
			{
				if((Dish.equalsIgnoreCase(Nouns) && (! CommonDishes.contains(Nouns))))
				CommonDishes.add(Nouns);
				
			}
				
		}
		
		
		//Initializing properties object to set annotator properties
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, lemma, sentiment");
		props.setProperty("ssplit.boundaryTokenRegex", "\\.");
		
		StanfordCoreNLP Pipeline = new StanfordCoreNLP(props);
		
		
		double SentimentIntensity;
		int SentimentScore;
		int Count;
		HashMap<String, String> DishScore = new HashMap<String, String>();
		
		//This loop calculates Sentiment Score and Sentiment Intensity for every dish
		for(String Dish : CommonDishes)
		{	   
			SentimentIntensity = 0;
			SentimentScore = 0;
			Count = 0;		
			
			StringBuilder RequiredText = new StringBuilder();
			for (String TextPart : TextString.toString().split("\\.")) {
			
				if(TextPart.contains(Dish))
				{
					RequiredText.append(TextPart).append(". ");
				}
				
			}
			
			//Initializing required objects for sentiment analysis
			Annotation annotation = Pipeline.process(RequiredText.toString());
			List<CoreMap> Sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
			
			//This loop calculates Sentiment for every line of text 				
			for (CoreMap Sentence : Sentences) {
				Tree Tree =	 Sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
				 int Sentiment = RNNCoreAnnotations.getPredictedClass(Tree);
			if (Sentiment==2)	
				Sentiment+=1;
			System.out.println(Sentiment + "\t" + Sentence);
			SentimentIntensity += Sentiment + 1; 
			Count = Count + 1;
			
			switch (Sentiment)
			{	case (2):
				case (3):
				case (4):
					SentimentScore = SentimentScore + 1;
					break;
				case (0):
				case (1):
					SentimentScore = SentimentScore - 1;
					break;			
			}			
			}
			
			if(SentimentIntensity != 0)
			{
				SentimentIntensity = Math.ceil(SentimentIntensity/Count);
			}

			DishScore.put(Dish, SentimentIntensity + " : "+ SentimentScore );
		}
		
		//Storing value for every dish in a HashMap 
		for(Map.Entry<String, String> entry : DishScore.entrySet())
		{	

			System.out.println(entry.getKey()+ " : " + entry.getValue());
		
		}
		
		return DishScore;
	}
	
	 /* This method is used to find the text for every item in dishes list 
		 * @param fileString : Reviews + tips of restaurants 
		 * @param dishes : Feature set of particular business 
		 * @throws IOException
		 */
	public static StringBuilder findPattern(List<String> Dishes, String fileString) {
		String regex = String.format("\\b(%s)\\w*\\b", StringUtils.join(Dishes, "|"));
		Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher m;
		StringBuilder DetectedList = new StringBuilder(); 
		for (String TextPart : fileString.split("\\.")) {
			m = p.matcher(TextPart);
			if (m.find()) {
				DetectedList.append(TextPart).append(". ");
			}
		}
		return DetectedList;
	}
}