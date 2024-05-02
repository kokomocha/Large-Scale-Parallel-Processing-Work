package cardinality;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.NullWritable;
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

public class maxEdgeRemain extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(maxEdgeRemain.class);

    public enum Counters{
        RemainingEdges
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf1 = getConf();
        conf1.setLong("maxValue", 500);
        final Job job1 = Job.getInstance(conf1, "Remaining Edges Finder");
        job1.setJarByClass(maxEdgeRemain.class);
        final Configuration jobConf1 = job1.getConfiguration();
        jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");
        job1.setMapperClass(ListMapper.class);
        job1.setCombinerClass(MaxEdgeReducer.class);
        job1.setReducerClass(MaxEdgeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (!job1.waitForCompletion(true)) {
            job1.getCounters().findCounter(Counters.RemainingEdges).getValue();
        }
        return job1.waitForCompletion(true) ? 0 : 1;
    }

    // Defining Incoming and Outgoing Edge Node wrt Middle Node(Key)
    public static class ListMapper extends Mapper<Object, Text, Text, Text> {
        
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
                if (!(Long.parseLong(nodes[0])<=Long.parseLong(MaxVal)) || !(Long.parseLong(nodes[1])<=Long.parseLong(MaxVal))){
                    context.write(new Text(nodes[0]),new Text(edge));//OutsideEdge
                    }
                }
            }
        }

    public static class MaxEdgeReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, final Context context) throws IOException, InterruptedException{
            long count=0;
            //Traverses through total input once
            for (Text val:values){
                count++;
            }
            context.getCounter(Counters.RemainingEdges ).increment(count);
            context.write(new Text("Remaining Edges"),new Text("Global Count:"+context.getCounter(Counters.RemainingEdges).getValue()));
            }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new Error("Usage: Paths-> <input> <output>");
        }
        try {
            ToolRunner.run(new maxEdgeRemain(), args);          
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
