package task2;

import java.io.IOException;
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

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class MenuFinder {

//	public static void main(String[] args) throws IOException {
//		MenuFinder obj1 = new MenuFinder();
//		List<String> dishes = new ArrayList<>();
//		obj1.getMenu("", dishes);
//	}

	public HashMap<String, String> getMenu(String Reviews, List<String> dishes)
			throws IOException {
//		Path filepath = Paths.get("C:/Users/Saurabh/Desktop/Task 2 data/Reviews.txt"); 
//		byte[] fileArray = Files.readAllBytes(filepath);
//		String fileString = new String(fileArray);
//		fileString = fileString.replace("\\n", "").replaceAll("\\?|!|\", \"", ". ");	
//		StringBuilder DetectedList = new StringBuilder();
//		List<String> dishes = new ArrayList<String>();
//		dishes.add("beef");
//		dishes.add("mein");
//		dishes.add("chow");
//		dishes.add("authentic");
//		dishes.add("cantonese");
//		dishes.add("roasted");
//		dishes.add("rice");
//		dishes.add("pork");
//		dishes.add("roast");
//		dishes.add("duck");
		
		StringBuilder DetectedList = new StringBuilder();
		System.out.println("Initial list of dishes \t" + dishes);
		
		DetectedList = findPattern(dishes, Reviews);
	//	System.out.println("Detected list: " + DetectedList);
		FindNouns obj = new FindNouns();
		
		String word = new String();
		String TextString = DetectedList.toString();
				
		
		for (int i = 0; i < dishes.size(); i++) {
			word = dishes.get(i);
			if (word.length() > 2) {
				String regex = String.format("(?i)(%s)", word);
				TextString = TextString.toString().replaceAll(regex, WordUtils.capitalize(word));	
			}
		}
		
	//	System.out.println("Nouns Capitlized list: " + TextString);
		
		List<String> NounsDetected = new ArrayList<String>();
		NounsDetected = (obj.getNouns(TextString));
		
		List<String> CommonDishes = new ArrayList<String>();
  		
		System.out.println("Nouns list Found : " + NounsDetected);
		for(String dish : dishes)
		{
			for(String  Nouns : NounsDetected)
			{
				if((dish.equalsIgnoreCase(Nouns) && (! CommonDishes.contains(Nouns))))
				CommonDishes.add(Nouns);
				
			}
				
		}
		
		System.out.println("Common Dishes " + CommonDishes);

		Properties props = new Properties();
		
		props.setProperty("annotators", "tokenize, ssplit, parse, lemma, sentiment");
		props.setProperty("ssplit.boundaryTokenRegex", "\\.");
		
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		
		
		double SentimentIntensity;
		int PreferenceScore;
		int Count;
		
		HashMap<String, String> DishScore = new HashMap<String, String>();
		
		
		for(String dish : CommonDishes)
			
		{   
			SentimentIntensity = 0;
			PreferenceScore = 0;
			Count = 0;		
			
			StringBuilder sb = new StringBuilder();
			for (String textPart : TextString.toString().split("\\.")) {
			
				if(textPart.contains(dish))
				{
					sb.append(textPart).append(". ");
				}
				
			}
			
			
			Annotation annotation = pipeline.process(sb.toString());
			List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
									
			for (CoreMap sentence :  sentences) {
				Tree tree =	 sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
				 int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
			//	String sentiment = sentence
			//			.get(SentimentCoreAnnotations.ClassName.class);
			if (sentiment==2)	
				sentiment+=1;
			System.out.println(sentiment + "\t" + sentence);
			SentimentIntensity += sentiment + 1; 
			Count = Count + 1;
			
			switch (sentiment)
			{	case (2):
				case (3):
				case (4):
					PreferenceScore = PreferenceScore + 1;
					break;
				case (0):
				case (1):
					PreferenceScore = PreferenceScore - 1;
					break;
				
			}			
			
			}
			
			if(SentimentIntensity != 0)
			{
				SentimentIntensity = Math.ceil(SentimentIntensity/Count);
			}
			
			//System.out.println(SentimentIntensity + "\tThis is the SentimentIntensity score for " + dish);
			//System.out.println(PreferenceScore + "\tThis is the Preference score for " + dish);
			
			DishScore.put(dish, SentimentIntensity + " : "+ PreferenceScore );
			
		}
		
		System.out.println("Below values are from Hasmap :");
		for(Map.Entry<String, String> entry : DishScore.entrySet())
		{	

			System.out.println(entry.getKey()+ " : " + entry.getValue());   // Print order- Dishname : SentimentIntensity : PreferenceScore
		
		}
		
		return DishScore;
	}
	

	public static StringBuilder findPattern(List<String> dishes, String fileString) {

		String regex = String.format("\\b(%s)\\w*\\b", StringUtils.join(dishes, "|"));
		Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		Matcher m;
		StringBuilder DetectedList = new StringBuilder(); 
		for (String textPart : fileString.split("\\.")) {
			m = p.matcher(textPart);
			if (m.find()) {
				DetectedList.append(textPart).append(". ");
			}
		}
		return DetectedList;
	}
}