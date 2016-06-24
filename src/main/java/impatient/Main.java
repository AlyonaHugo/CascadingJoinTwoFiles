package main.java.impatient;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.*;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.MultiSinkTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class Main {
    public static void main(String[] args) {
        String docPath = args[0];
        String wcPath = args[1];
        String doc2Path = args[2];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        AppProps.setApplicationName(properties, "Part 1");
        AppProps.addApplicationTag(properties, "lets:do:it");
        AppProps.addApplicationTag(properties, "technology:Cascading");
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

// create source and sink taps
        Tap wcTap = new Hfs(new TextDelimited(true, ","), wcPath);


        Fields classInterfaceFiles = new Fields("interface", "class");
        Tap classInterfaceTap = new Hfs(new TextDelimited(classInterfaceFiles, true, ","), docPath);

        Fields classAmountFields = new Fields("class1", "amount");
        Tap classAmountFileTap = new Hfs(new TextDelimited(classAmountFields, true, ","), doc2Path);

        Pipe classInterfaceFilePipe = new Pipe("classInterfaceFilePipe");

        Pipe classIAmountFilePipe = new Pipe("classIAmountFilePipe");

        Fields groupFields = new Fields("class");
        Fields groupFields1 = new Fields("class1"); // fields used as joining keys
        Pipe outPipe = new CoGroup(classInterfaceFilePipe, groupFields, classIAmountFilePipe, groupFields1, new InnerJoin());

        // build flow definition
        FlowDef flowDef = FlowDef.flowDef().setName("myFlow")
                .addSource(classInterfaceFilePipe, classInterfaceTap)
                .addSource(classIAmountFilePipe, classAmountFileTap)
                .addTailSink(outPipe, wcTap);
        //   .addTailSink( outPipe, wcTap );


// write a DOT file and run the flow
        Flow wcFlow = flowConnector.connect(flowDef);
        wcFlow.writeDOT("dot/wc.dot");
        wcFlow.complete();
    }
}
