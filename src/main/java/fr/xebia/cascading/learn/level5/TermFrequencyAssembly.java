package fr.xebia.cascading.learn.level5;

import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;

public class TermFrequencyAssembly extends SubAssembly {

  public static final Fields TF = new Fields("tf");
  public static final Fields TF_TOKEN = new Fields("tf_token");
  public static final Fields TF_COUNT = new Fields("tf_count");

  public TermFrequencyAssembly(final Pipe previous, final Fields docIdField, final Fields tokenField) {
    super(previous);

    // calculate tf count
    Pipe tfPipe = new CountBy(previous, Fields.join(docIdField, tokenField), TF_COUNT);
    tfPipe = new Rename(tfPipe, tokenField, TF_TOKEN);

    Pipe maxTermFrequencyPipe = new MaxTermFrequencyAssembly(tfPipe, docIdField, TF_COUNT);

    // join tf count with tf max
    tfPipe = new CoGroup(tfPipe, docIdField, maxTermFrequencyPipe, MaxTermFrequencyAssembly.MAX_TF_ID);
    tfPipe = new Discard(tfPipe, MaxTermFrequencyAssembly.MAX_TF_ID);

    // calculate tf
    ExpressionFunction tfExpression = new ExpressionFunction(TF, "(double) tf_count / tf_max", Double.class);
    Fields tfArguments = Fields.join(TF_COUNT, MaxTermFrequencyAssembly.MAX_TF);
    tfPipe = new Each(tfPipe, tfArguments, tfExpression, Fields.ALL);

    setTails(tfPipe);
  }

}
