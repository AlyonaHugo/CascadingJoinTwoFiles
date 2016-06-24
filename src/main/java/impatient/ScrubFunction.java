
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class ScrubFunction extends BaseOperation implements Function
{
    public ScrubFunction( Fields fieldDeclaration )
    {
        super( 2, fieldDeclaration );
    }

    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
        TupleEntry argument = functionCall.getArguments();
        String interface1 = argument.getString( 0 );
        System.out.println("interface " + interface1 );
        Integer amount = new Integer(argument.getString( 1 ));
        System.out.println("amount " + amount);
        Tuple result = new Tuple();
        result.add( interface1 );
        result.add( amount );
        functionCall.getOutputCollector().add( result );
    }

    public String scrubText( String text )
    {
        return text.trim().toLowerCase();
    }
}