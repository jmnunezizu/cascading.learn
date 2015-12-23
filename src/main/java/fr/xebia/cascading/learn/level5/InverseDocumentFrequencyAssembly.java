package fr.xebia.cascading.learn.level5;

import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;

public class InverseDocumentFrequencyAssembly extends SubAssembly {

  private static final Fields LHS_JOIN = new Fields("lhs_join");
  private static final Fields RHS_JOIN = new Fields("rhs_join");

  public static final Fields DF_TOKEN = new Fields("df_token");
  public static final Fields DF_COUNT = new Fields("df_count");

  public InverseDocumentFrequencyAssembly(final Pipe previous, final Fields docIdField, final Fields tokenField) {
    super(previous);

    Pipe idfPipe = new Pipe("Inverse Document Frequency", previous);

    Pipe dPipe = new TotalDocumentsAssembly(idfPipe, docIdField);

    // one branch tallies the token counts for document frequency (DF)
    Pipe dfPipe = new Unique("DF", idfPipe, Fields.ALL);
    dfPipe = new CountBy(dfPipe, tokenField, DF_COUNT);

    dfPipe = new Rename(dfPipe, tokenField, DF_TOKEN);
    dfPipe = new Each(dfPipe, new Insert(LHS_JOIN, 1), Fields.ALL);


    // join DF and D
    idfPipe = new HashJoin(dfPipe, LHS_JOIN, dPipe, RHS_JOIN);

    setTails(idfPipe);
  }

}
