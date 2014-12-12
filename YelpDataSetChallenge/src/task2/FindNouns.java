package task2;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.Tokenizer;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreebankLanguagePack;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class FindNouns {

	public List<String> getNouns(String Text) {
	//	 public static void main (String [] args){
		LexicalizedParser lp = LexicalizedParser
				.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");
		lp.setOptionFlags(new String[] { "-maxLength", "5000","-retainTmpSubcategories" });
	
		
		
		TreebankLanguagePack tlp = lp.getOp().langpack();
		List<String> AllTaggedWords = new ArrayList<String>();
		for (String SplitText : Text.split("\\."))
		{
		Tokenizer<? extends HasWord> toke = tlp.getTokenizerFactory()
				.getTokenizer(new StringReader(SplitText));
		List<? extends HasWord> sentence2 = toke.tokenize();
		Tree parse = (Tree) lp.apply(sentence2);
//		System.out.println(parse);
		List taggedWords = parse.taggedYield();
//		System.out.println(taggedWords);
		List<Tree> phraseList = new ArrayList<Tree>();
		List<String> nouns = new ArrayList<String>();
		for (Tree subtree : parse) {

			if (subtree.label().value().contains("NN")) {
				phraseList.add(subtree);
				nouns.add(subtree.toString().replaceAll("NN |NNP |NNS |NNPS |\\(|\\)", ""));
			}
		}
		
//		System.out.println("We are nouns :D " + nouns);
		AllTaggedWords.addAll(nouns);
		
		}

		
		
		return AllTaggedWords;

	}
}