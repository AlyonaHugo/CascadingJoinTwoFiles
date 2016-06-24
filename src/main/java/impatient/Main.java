

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.aggregator.Sum;
import cascading.pipe.*;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.commons.io.FileUtils;

public class Main {
    public static void main(String[] args) {
        String docPath = args[0];
        String wcPath = args[1];
        String doc2Path = args[2];

        cleanDirectory(wcPath);

        FlowConnector flowConnector = createFlowConnector();

        // create source and sink taps
        Tap wcTap = new Hfs(new TextDelimited(true, ","), wcPath);

        Fields showChannelFiles = new Fields("channel", "show");
        Tap showChannelTap = new Hfs(new TextDelimited(showChannelFiles, true, ","), docPath);

        Fields showAmountFields = new Fields("show1", "amount");
        Tap showAmountFileTap = new Hfs(new TextDelimited(showAmountFields, true, ","), doc2Path);

        Pipe showChannelFilePipe = new Pipe("showChannelFilePipe");
        Pipe showIAmountFilePipe = new Pipe("showIAmountFilePipe");

        Pipe outPipe = joinAndLeftTwoFiels(showChannelFilePipe, showIAmountFilePipe);
        Pipe wcPipe = summarize(outPipe);


        // build flow definition
        FlowDef flowDef = FlowDef.flowDef().setName("myFlow")
                .addSource(showChannelFilePipe, showChannelTap)
                .addSource(showIAmountFilePipe, showAmountFileTap)
                .addTailSink( wcPipe, wcTap );


        // write a DOT file and run the flow
        Flow wcFlow = flowConnector.connect(flowDef);
        wcFlow.writeDOT("dot/wc.dot");
        wcFlow.complete();
    }

    private static void cleanDirectory(String wcPath) {
        try {
            File f = new File(wcPath);
            FileUtils.cleanDirectory(f); //clean out directory (this is optional -- but good know)
            FileUtils.forceDelete(f); //delete directory
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static FlowConnector createFlowConnector() {
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        AppProps.setApplicationName(properties, "Part 1");
        AppProps.addApplicationTag(properties, "lets:do:it");
        AppProps.addApplicationTag(properties, "technology:Cascading");
        return new Hadoop2MR1FlowConnector(properties);
    }

    private static Pipe joinAndLeftTwoFiels(Pipe showChannelFilePipe, Pipe showIAmountFilePipe) {
        Fields groupFields = new Fields("show");
        Fields groupFields1 = new Fields("show1"); // fields used as joining keys
        Pipe outPipe = new CoGroup(showChannelFilePipe, groupFields, showIAmountFilePipe, groupFields1, new InnerJoin());


        // define "ScrubFunction" to clean up the token stream
        Fields scrubArguments = new Fields( "channel", "amount" );
        outPipe = new Each(outPipe, scrubArguments, new ScrubFunction( scrubArguments ), Fields.RESULTS );
        return outPipe;
    }

    private static Pipe summarize(Pipe outPipe) {
        Fields channelFields= new Fields("channel");
        Fields amount = new Fields("amount");
        Pipe wcPipe = new Pipe( "wc", outPipe );
        wcPipe = new GroupBy( wcPipe, channelFields);
        wcPipe = new Every( wcPipe, amount, new Sum(), Fields.ALL );
        return wcPipe;
    }
}
