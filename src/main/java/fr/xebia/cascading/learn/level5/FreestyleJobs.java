package fr.xebia.cascading.learn.level5;

import cascading.flow.FlowDef;
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
		final Fields lowercaseLine = new Fields("lowercaseLine");
		final Fields id = new Fields("id");
		final Fields token = new Fields("token");
		final Fields rawToken = new Fields("raw_token");

		ExpressionFunction toLowerCaseFn = new ExpressionFunction(lowercaseLine, "content.toLowerCase().trim()", String.class);
		pipe = new Each(pipe, content, toLowerCaseFn, Fields.join(id, lowercaseLine));

		RegexSplitGenerator splitter = new RegexSplitGenerator(rawToken, "[/'\\s]");
		pipe = new Each(pipe, lowercaseLine, splitter, Fields.join(id, rawToken));

		// sanitise words
		RegexReplace sanitiseFn = new RegexReplace(token, "[^a-zA-Z-]+", "");
		pipe = new Each(pipe, rawToken, sanitiseFn, Fields.join(id, token));

		// -----

		final Pipe tfPipe = new TermFrequencyAssembly(pipe, id, token);
		final Pipe idfPipe = new InverseDocumentFrequencyAssembly(pipe, id, token);

		// the IDF side of the join is smaller, so it goes on the RHS
		Pipe tfidfPipe = new CoGroup(tfPipe, TermFrequencyAssembly.TF_TOKEN, idfPipe, InverseDocumentFrequencyAssembly.DF_TOKEN);

		final Fields tfidf = new Fields("tfidf");
		ExpressionFunction tfidfExpression = new ExpressionFunction(tfidf, "(double) tf * idf", Double.class);
		Fields tfidfArguments = Fields.join(TermFrequencyAssembly.TF, InverseDocumentFrequencyAssembly.IDF);
		Fields tfidOutput = Fields.join(id, TermFrequencyAssembly.TF_TOKEN, tfidf);
		tfidfPipe = new Each(tfidfPipe, tfidfArguments, tfidfExpression, tfidOutput);

		// filter and clean up
		tfidfPipe = new Each(tfidfPipe, tfidf, new ExpressionFilter("tfidf < 0.1", Double.class));
		tfidfPipe = new Rename(tfidfPipe, Fields.join(id, tfidf, TermFrequencyAssembly.TF_TOKEN), new Fields("docId", "tfidf", "word"));

		// sort by docId
		tfidfPipe = new GroupBy(tfidfPipe, Fields.ALL, Fields.ALL, true);

		return FlowDef.flowDef()
				.setName("td-idf")
				.addSource(tfidfPipe, source)
				.addTailSink(tfidfPipe, sink);
	}
	
}
