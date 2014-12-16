package InfoTask2;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.process.Tokenizer;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreebankLanguagePack;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class FindNouns {
	 /* This method is used to find nouns from a given text
	 * @param Text : Reviews + Tips text of the restaurant
	 * @throws IOException
	 */
	public List<String> getNouns(String Text) throws IOException{
		//Initializing objects for parsing the text
		LexicalizedParser lp = LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.caseless.ser.gz");
		lp.setOptionFlags(new String[] { "-maxLength", "1000","-retainTmpSubcategories" });
		TreebankLanguagePack tlp = lp.getOp().langpack();
		List<String> AllTaggedWords = new ArrayList<String>();
		
		//Splitting text into Sentences for parsing
		for (String SplitText : Text.split("\\."))
		{
		Tokenizer<? extends HasWord> Toke = tlp.getTokenizerFactory().getTokenizer(new StringReader(SplitText));
		List<? extends HasWord> Sentence = Toke.tokenize();
		Tree Parse = (Tree) lp.apply(Sentence);
		List TaggedWords = Parse.taggedYield();
		List<Tree> PhraseList = new ArrayList<Tree>();
		List<String> Nouns = new ArrayList<String>();
		
		// Adding words tagged as nouns to List
		for (Tree SubTree : Parse) {
			if (SubTree.label().value().contains("NN")) {
				PhraseList.add(SubTree);
				Nouns.add(SubTree.toString().replaceAll("NN |NNP |NNS |NNPS |\\(|\\)", ""));
			}
		}
		AllTaggedWords.addAll(Nouns);
		}
		return AllTaggedWords;

	}
}