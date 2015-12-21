package fr.xebia.cascading.learn.level4;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {

  /**
   * Use {@link CoGroup} in order to know the party of each presidents.
   * You will need to create (and bind) one Pipe per source.
   * You might need to correct the schema in order to match the expected results.
   * <p/>
   * presidentsSource field(s) : "year","president"
   * partiesSource field(s) : "year","party"
   * sink field(s) : "president","party"
   *
   * @see http://docs.cascading.org/cascading/3.0/userguide/ch05-pipe-assemblies.html#_cogroup
   */
  public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource, Tap<?, ?, ?> partiesSource, Tap<?, ?, ?> sink) {
    Pipe presidents = new Pipe("presidents");
    Pipe parties = new Pipe("parties");

    Fields joinField = new Fields("year");

    Pipe join = new CoGroup(presidents, joinField, parties, joinField, new Fields("year", "president", "year1", "party"));
    join = new Retain(join, new Fields("president", "party"));

    return FlowDef.flowDef()
        .addSource(presidents, presidentsSource)
        .addSource(parties, partiesSource).addTail(join)
        .addTail(join)
        .addSink(join, sink);
  }

  /**
   * Split the input in order use a different sink for each party. There is no
   * specific operator for that, use the same Pipe instance as the parent.
   * You will need to create (and bind) one named Pipe per sink.
   * <p/>
   * source field(s) : "president","party"
   * gaullistSink field(s) : "president","party"
   * republicanSink field(s) : "president","party"
   * socialistSink field(s) : "president","party"
   * <p/>
   * In a different context, one could use {@link PartitionTap} in order to arrive to a similar results.
   *
   * @see http://docs.cascading.org/cascading/3.0/userguide/ch15-advanced.html#partition-tap
   */
  public static FlowDef split(Tap<?, ?, ?> source, Tap<?, ?, ?> gaullistSink, Tap<?, ?, ?> republicanSink,
      Tap<?, ?, ?> socialistSink) {

    Pipe presidents = new Pipe("presidents");

    ExpressionFilter gaullistFilter = new ExpressionFilter("!party.equals(\"Gaullist\")", String.class);
    ExpressionFilter republicanFilter = new ExpressionFilter("!party.equals(\"Republican\")", String.class);
    ExpressionFilter socialistFilter = new ExpressionFilter("!party.equals(\"Socialist\")", String.class);

    Pipe gaullist = new Pipe("gaullist", presidents);
    Pipe republican = new Pipe("republican", presidents);
    Pipe socialist = new Pipe("socialist", presidents);

    gaullist = new Each(gaullist, new Fields("president", "party"), gaullistFilter);
    republican = new Each(republican, new Fields("president", "party"), republicanFilter);
    socialist = new Each(socialist, new Fields("president", "party"), socialistFilter);

    return FlowDef.flowDef()
        .addSource(presidents, source)
        .addTail(gaullist)
        .addTail(republican)
        .addTail(socialist)
        .addSink(gaullist, gaullistSink)
        .addSink(republican, republicanSink)
        .addSink(socialist, socialistSink);
  }

}
