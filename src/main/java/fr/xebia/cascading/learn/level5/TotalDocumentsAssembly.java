package fr.xebia.cascading.learn.level5;

import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;

public class TotalDocumentsAssembly extends SubAssembly {

  private static final Fields TALLY = new Fields("tally");
  private static final Fields RHS_JOIN = new Fields("rhs_join");
  private static final Fields TOTAL_DOCS = new Fields("n_docs");

  public TotalDocumentsAssembly(final Pipe previous, final Fields docIdField) {
    super(previous);

    Pipe dPipe = new Unique("D", previous, docIdField);
    dPipe = new Each(dPipe, new Insert(TALLY, 1), Fields.ALL);
    dPipe = new Each(dPipe, new Insert(RHS_JOIN, 1), Fields.ALL);
    dPipe = new SumBy(dPipe, RHS_JOIN, TALLY, TOTAL_DOCS, long.class);

    setTails(dPipe);
  }

}
