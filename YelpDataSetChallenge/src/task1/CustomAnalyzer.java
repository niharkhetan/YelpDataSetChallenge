package task1;



import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;

public final class CustomAnalyzer extends StopwordAnalyzerBase {
//	public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;
//	private int maxTokenLength;
	public static final CharArraySet STOP_WORDS_SET = StopAnalyzer.ENGLISH_STOP_WORDS_SET;

	public CustomAnalyzer(CharArraySet stopWords) {
		super(stopWords);

		//this.maxTokenLength = 255;
	}



	public CustomAnalyzer() {
		this(STOP_WORDS_SET);
	}



	public CustomAnalyzer(Reader stopwords) throws IOException {
		this(loadStopwordSet(stopwords));
	}

	

//	public void setMaxTokenLength(int length) {
//		this.maxTokenLength = length;
//	}
//
//	public int getMaxTokenLength() {
//		return this.maxTokenLength;
//	}

	protected Analyzer.TokenStreamComponents createComponents(String fieldName,
			Reader reader) {
		StandardTokenizer src = new StandardTokenizer(getVersion(), reader);
		TokenStream tok = new StandardFilter(getVersion(), src);
		
		tok = new StopFilter(getVersion(), tok, this.stopwords);
		tok= new ShingleFilter(tok,2,2);
		return new Analyzer.TokenStreamComponents(src, tok);
		};
	}
