package fr.xebia.cascading.learn.level5;

import cascading.operation.aggregator.First;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Calculates the maximum term frequency per document.
 */
public class MaxTermFrequencyAssembly extends SubAssembly {

  public static final Fields MAX_TF_ID = new Fields("maxtf_id");
  public static final Fields MAX_TF = new Fields("tf_max");

  public MaxTermFrequencyAssembly(final Pipe previous, final Fields docIdField, final Fields tfCountField) {
    super(previous);

    Pipe maxPerDocument = new Pipe("maxTermFrequency", previous);
    maxPerDocument = new GroupBy(maxPerDocument, docIdField, tfCountField, true);
    maxPerDocument = new Every(maxPerDocument, Fields.join(docIdField, tfCountField), new First(Fields.join(MAX_TF_ID, MAX_TF)), Fields.RESULTS);

    setTails(maxPerDocument);
  }

}
