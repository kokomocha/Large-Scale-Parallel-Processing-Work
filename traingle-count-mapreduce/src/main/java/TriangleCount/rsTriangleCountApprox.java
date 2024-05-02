package TriangleCount;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

public class rsTriangleCountApprox extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(rsTriangleCountApprox.class);

    private enum Counters {
        // Custom counter to complete in 2 jobs instead of 3.
        Count3TrianglesRS
      }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf1 = getConf();
        final Job job1 = Job.getInstance(conf1, "2Path Finder");
        conf1.setLong("maxValue", 50000);
        job1.setJarByClass(rsTriangleCountApprox.class);
        final Configuration jobConf1 = job1.getConfiguration();
        jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");
        job1.setMapperClass(rsTriangleCountApprox.NodeMapper.class);
        job1.setReducerClass(rsTriangleCountApprox.EdgeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (!job1.waitForCompletion(true)) {
            return 1;
            }
        //Job2
        Configuration conf2 = new Configuration();
        final Job job2 = Job.getInstance(conf2, "3Path Counter");
        job2.setJarByClass(rsTriangleCountApprox.class);
        final Configuration jobConf2 = job1.getConfiguration();
        jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");
        job2.setMapperClass(rsTriangleCountApprox.NodeMapper.class);
        job2.setReducerClass(rsTriangleCountApprox.EdgeReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        if (!job2.waitForCompletion(true)) {
            job2.getCounters().findCounter(Counters.Count3TrianglesRS).getValue();
        }
        return job2.waitForCompletion(true) ? 0 : 1;
    }

    // Defining Incoming and Outgoing Edge Node wrt Middle Node(Key)
    public static class NodeMapper extends Mapper<Object, Text, Text, Text> {
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
                if (nodes.length==2){
                    if (Long.parseLong(nodes[0])<=Long.parseLong(MaxVal) && Long.parseLong(nodes[1])<=Long.parseLong(MaxVal)){
                        context.write(new Text(nodes[0]),new Text("O-"+edge));//OutsideEdge
                        context.write(new Text(nodes[1]),new Text("I-"+edge));//InsideEdge
                    }
                }else{
                    context.write(new Text(nodes[0]),new Text("O-"+nodes[0]+","+nodes[1]));//OutsideEdge
                    context.write(new Text(nodes[1]),new Text("O-"+nodes[1]+","+nodes[2]));//OutsideEdge
                    context.write(new Text(nodes[2]),new Text("C-"+edge));//Check endpoints
                }
            }
        }
	}

    public static class EdgeReducer extends Reducer<Text, Text, Text, NullWritable>{
        @Override
        public void reduce(Text key, Iterable<Text> values, final Context context) throws IOException, InterruptedException{
            //Use HashSet for unique values only
            HashSet<String> outNode = new HashSet<String>();//cardinalty m
            HashSet<String> inEdge = new HashSet<String>();//cardinality n
            long count=0;
            //Traverses through total input once
            for (Text val:values){
                String[] split = val.toString().split("-");
                String[] parts = split[1].split(",");
                if(parts.length==3){
                    inEdge.add(parts[0]+","+parts[1]);
                }else{//for 2 nodes
                    if (split[0].equals("I")){
                        inEdge.add(parts[0]);//input-m2
                    }else {
                        outNode.add(split[1]);//m2
                    }
                }
            }
            for (String node:outNode){
                for (String in: inEdge){
                    String[] in_0 = in.split(",");
                    String[] out_1 = node.split(",");
                    //if inEdge is single Node, avoid X-Y-X
                    if (in_0.length==1){
                        //if inEdge is single Node, avoid X-Y-X
                        if (!in_0[0].equals(out_1[1])){
                            context.write(new Text(in+","+node),NullWritable.get());
                        }
                    }else {//in_0.length==2
                        //Check if X-Y-B and B-X
                        if (in_0[0].equals(out_1[1])){
                            context.write(new Text(in+","+node),NullWritable.get());
                            count+=1;
                        }
                    }
                }
            }
            // Increment Triangle counts; Divide by 3!!
            context.getCounter(Counters.Count3TrianglesRS).increment(count);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
          throw new Error("Usage: Paths-> <input> <intermediate-output> <output>");
        }
        try {
          ToolRunner.run(new rsTriangleCountApprox(), args);          
        } catch (final Exception e) {
          logger.error("", e);
        }
    }
}