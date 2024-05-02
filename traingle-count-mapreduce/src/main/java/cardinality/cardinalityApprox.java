package cardinality;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger; 

public class cardinalityApprox extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(cardinalityApprox.class);

    public enum Counters{
        Cardinality_Approx
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf1 = getConf();
        conf1.setLong("maxValue", 172500);
        final Job job1 = Job.getInstance(conf1, "Approx Node-Cardinality Finder");
        job1.setJarByClass(cardinalityApprox.class);
        final Configuration jobConf1 = job1.getConfiguration();
        jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");
        job1.setMapperClass(EdgeMapper.class);
        job1.setReducerClass(CardinalityReducerApprox.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (!job1.waitForCompletion(true)) {
            job1.getCounters().findCounter(Counters.Cardinality_Approx).getValue();
        }
        return job1.waitForCompletion(true) ? 0 : 1;
    }

    // Defining Incoming and Outgoing Edge Node wrt Middle Node(Key)
    public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {
        
        private final Text edge = new Text();
        private String MaxVal;
        
        @Override
        public void setup(Context context) {
            this.MaxVal = context.getConfiguration().get("maxValue");
        }

        @Override
        public void map(final Object key, final Text value, final Context context)throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) {
				edge.set(itr.nextToken());
                final String[] nodes = edge.toString().split(",");
                if (Long.parseLong(nodes[0])<=Long.parseLong(MaxVal) && Long.parseLong(nodes[1])<=Long.parseLong(MaxVal)){
                    context.write(new Text(nodes[0]),new Text("O-"+edge));//OutsideEdge
                    context.write(new Text(nodes[1]),new Text("I-"+edge));//InsideEdge
                    }
                }
            }
        }

    public static class CardinalityReducerApprox extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, final Context context) throws IOException, InterruptedException{
            long m=0;
            long n=0;
            //Traverses through total input once
            for (Text val:values){
                String[] split = val.toString().split("-");
                if (split[0].equals("I")){
                    m+=1;//m
                }else {
                    n+=1;//n=input-m
                }
            }
            
            if (m!=0 && n!=0){
                //Inclusive of Hop Paths!!
                long p=m*n;
                context.getCounter(Counters.Cardinality_Approx ).increment(p);
                context.write(key,new Text(String.valueOf(p)+"("+String.valueOf(m)+","+String.valueOf(n)
                        +")"+"\t"+"Global Count:"+context.getCounter(Counters.Cardinality_Approx).getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Error("Usage: Paths-> <input> <output> <maxVal>");
        }
        try {
            ToolRunner.run(new cardinalityApprox(), args);          
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
