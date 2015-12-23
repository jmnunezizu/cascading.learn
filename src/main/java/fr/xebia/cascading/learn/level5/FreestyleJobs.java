package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowDef;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexReplace;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.*;
import cascading.pipe.assembly.*;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * You now know all the basics operators. Here you will have to compose them by yourself.
 */
public class FreestyleJobs {

	/**
	 * Word count is the Hadoop "Hello world" so it should be the first step.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "word","count"
	 */
	public static FlowDef countWordOccurences(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		// line -> toLowerCase -> split into words -> words -> remove special characters -> words -> groupBy -> count

		// I had to modify the expectation because IMO, the word v2 should be accepted as "v"

		Pipe pipe = new Pipe("wordCount");

		final Fields line = new Fields("line");
		final Fields word = new Fields("word");

		// convert to lowercase
		ExpressionFunction toLowerCaseFn = new ExpressionFunction(line, "line.toLowerCase()", String.class);
		pipe = new Each(pipe, line, toLowerCaseFn);

		// split lines into words
		RegexSplitGenerator splitter = new RegexSplitGenerator(word, "[/'\\s]");
		pipe = new Each(pipe, line, splitter, word);

		// sanitise words
		RegexReplace sanitiseFn = new RegexReplace(word, "[^a-zA-Z-]+", "");
		pipe = new Each(pipe, word, sanitiseFn);

		// group by word and count
		pipe = new GroupBy(pipe, word, Fields.ALL);
		pipe = new Every(pipe, word, new Count(), Fields.ALL);

		return FlowDef.flowDef()
				.setName("Word Count")
				.addSource(pipe, source)
				.addTail(pipe)
				.addSink(pipe, sink);
	}
	
	/**
	 * Now, let's try a non trivial job : td-idf. Assume that each line is a
	 * document.
	 * 
	 * source field(s) : "line"
	 * sink field(s) : "docId","tfidf","word"
	 * 
	 * <pre>
	 * t being a term
	 * t' being any other term
	 * d being a document
	 * D being the set of documents
	 * Dt being the set of documents containing the term t
	 * 
	 * tf-idf(t,d,D) = tf(t,d) * idf(t, D)
	 * 
	 * where
	 * 
	 * tf(t,d) = f(t,d) / max (f(t',d))
	 * ie the frequency of the term divided by the highest term frequency for the same document
	 * 
	 * idf(t, D) = log( size(D) / size(Dt) )
	 * ie the logarithm of the number of documents divided by the number of documents containing the term t 
	 * </pre>
	 * 
	 * Wikipedia provides the full explanation
	 * @see http://en.wikipedia.org/wiki/tf-idf
	 * 
	 * If you are having issue applying functions, you might need to learn about field algebra
	 * @see http://docs.cascading.org/cascading/3.0/userguide/ch04-tuple-fields.html#field-algebra
	 * 
	 * {@link First} could be useful for isolating the maximum.
	 * 
	 * {@link HashJoin} can allow to do cross join.
	 * 
	 * PS : Do no think about efficiency, at least, not for a first try.
	 * PPS : You can remove results where tfidf < 0.1
	 */
	public static FlowDef computeTfIdf(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
		Pipe pipe = new Pipe("tf-idf");

		final Fields content = new Fields("content");
		final Fields docId = new Fields("docId");
		final Fields word = new Fields("token");
		final Fields words = new Fields("tokens");
		final Fields lowercasedLine = new Fields("lowercasedLine");
		final Fields idAndWords = new Fields("id", "tokens");
		final Fields idAndWord = new Fields("id", "token");

		final Fields id = new Fields("id");
		final Fields token = new Fields("token");

		// convert to lowercase
		ExpressionFunction toLowerCaseFn = new ExpressionFunction(lowercasedLine, "content.toLowerCase().trim()", String.class);
		pipe = new Each(pipe, content, toLowerCaseFn, Fields.join(id, lowercasedLine));

		// split into words
		RegexSplitGenerator splitter = new RegexSplitGenerator(words, "[/'\\s]");
		pipe = new Each(pipe, lowercasedLine, splitter, idAndWords);

		// sanitise words
		RegexReplace sanitiseFn = new RegexReplace(word, "[^a-zA-Z-]+", "");
		pipe = new Each(pipe, words, sanitiseFn, idAndWord);

		// -----

		// one branch of the flow tallies the token counts for term frequency (TF)
		Pipe tfPipe = new Pipe("TF", pipe);
		Fields tfToken = new Fields("tf_token");
		Fields tfCount = new Fields("tf_count");
		tfPipe = new CountBy(tfPipe, Fields.join(id, token), tfCount);
		tfPipe = new Rename(tfPipe, token, tfToken);

		// max term frequency per document (new)
		final Fields maxTfId = new Fields("maxtf_id");
		final Fields maxTf = new Fields("max_tf");
		Pipe maxPerDocument = new Pipe("MAX_TF", tfPipe);
		maxPerDocument = new GroupBy(maxPerDocument, Fields.join(id), tfCount, true);
		maxPerDocument = new Every(maxPerDocument, Fields.join(id, tfCount), new First(Fields.join(maxTfId, maxTf)), Fields.RESULTS);

		// one branch counts the number of documents (D)
		final Fields tally = new Fields("tally");
		final Fields rhsJoin = new Fields("rhs_join");
		final Fields nDocs = new Fields("n_docs");
		Pipe dPipe = new Unique("D", pipe, id);
		dPipe = new Each(dPipe, new Insert(tally, 1), Fields.ALL);
		dPipe = new Each(dPipe, new Insert(rhsJoin, 1), Fields.ALL);
		dPipe = new SumBy(dPipe, rhsJoin, tally, nDocs, long.class);

		// one branch tallies the token counts for document frequency (DF)
		Pipe dfPipe = new Unique("DF", pipe, Fields.ALL);
		Fields dfCount = new Fields("df_count");
		dfPipe = new CountBy(dfPipe, token, dfCount);

		Fields dfToken = new Fields("df_token");
		Fields lhsJoin = new Fields("lhs_join");
		dfPipe = new Rename(dfPipe, token, dfToken);
		dfPipe = new Each(dfPipe, new Insert(lhsJoin, 1), Fields.ALL);

		// join to bring together all the components for calculating TF-IDF
		// the D side of the join is smaller, so it goes on the RHS
		Pipe idfPipe = new HashJoin(dfPipe, lhsJoin, dPipe, rhsJoin);

		// the IDF side of the join is smaller, so it goes on the RHS
		Pipe tfidfPipe = new CoGroup(tfPipe, tfToken, idfPipe, dfToken);

		// join the max TF
		tfidfPipe = new CoGroup(tfidfPipe, id, maxPerDocument, maxTfId);
		tfidfPipe = new Discard(tfidfPipe, maxTfId);
		// ----------

		// calculate the TF-IDF weights, per token, per document
		Fields tfidf = new Fields("tfidf");
		String expression = "(double) (tf_count / max_tf) * Math.log((double) n_docs / (df_count))";
		ExpressionFunction tfidfExpression = new ExpressionFunction(tfidf, expression, Double.class);
		Fields tfidfArguments = new Fields("tf_count", "df_count", "n_docs", "max_tf");
		tfidfPipe = new Each(tfidfPipe, tfidfArguments, tfidfExpression, Fields.ALL);

		Fields fieldSelector = new Fields("tf_token", "id", "tfidf");
		tfidfPipe = new Retain(tfidfPipe, fieldSelector);
		tfidfPipe = new Rename(tfidfPipe, tfToken, token);

		ExpressionFilter tfidfFilter = new ExpressionFilter("tfidf < 0.1", Double.class);
		tfidfPipe = new Each(tfidfPipe, tfidf, tfidfFilter);
		tfidfPipe = new Rename(tfidfPipe, new Fields("id", "tfidf", "token"), new Fields("docId", "tfidf", "word"));

		tfidfPipe = new GroupBy(tfidfPipe, new Fields("docId", "tfidf", "word"), Fields.ALL, true);

		return FlowDef.flowDef()
				.setName("td-idf")
				.addSource(tfidfPipe, source)
				.addTailSink(tfidfPipe, sink);
	}
	
}
