package TriangleCount;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class repTriangleApprox extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(repTriangleApprox.class);

  private enum Counters {
    // Custom counter to complete in 2 jobs instead of 3.
    Count3TrianglesRep 
  }

  public int run(String[] args) throws Exception {
    //final Configuration conf1 = getConf();
    Configuration conf = getConf();
    final Job job1 = Job.getInstance(conf, "RepJoin Path-Finder");
    conf.setLong("maxValue", 150000);
    job1.setJarByClass(repTriangleApprox.class);
    final Configuration jobConf1 = job1.getConfiguration();
    jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");
    job1.setJarByClass(repTriangleApprox.class);
    job1.setMapperClass(RepMapper.class);
    job1.setNumReduceTasks(0);//Explicitly Setting reducers 0
    job1.setOutputKeyClass(NullWritable.class);
    job1.setOutputValueClass(Text.class);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "2048");//default increased
    job1.getConfiguration().set("mapred.task.timeout", "1000000");//default increased
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    //job1.addCacheFile(filesys.getPath().toUri());
    return job1.waitForCompletion(true) ? 0 : 1;
  }

  public static class RepMapper extends Mapper<Object, Text, NullWritable, Text> {
    //HashMap[Node1]->Node2,Node3..Node n for Braodcast
    private HashMap<String, HashSet<String>> broadcastMap = new HashMap<>();
    private String MaxValRep;
    
    //Setup
    public void setup(Context context) throws IOException {
      //Max Value for controlling Compute Costs
      this.MaxValRep = context.getConfiguration().get("maxValue");

      Configuration conf = context.getConfiguration();
      //Caching Edges.csv
      URI[] CacheFile = Job.getInstance(conf).getCacheFiles();

      //Exception for null or non-exitant Cache
      if (CacheFile.length == 0 || CacheFile == null) {
        throw new IOException("File does not exist in the cache.");
      }
      //Check all existing File Caches
      for (URI CacheURI : CacheFile) {
        Path cacheFilePath = new Path(CacheURI);
        processCacheFile(cacheFilePath, conf);
      }
    }

    private void processCacheFile(Path cacheFilePath, Configuration conf) throws IOException {
      // Reading Cache files line by line.
      try (FileSystem filesys = FileSystem.get(cacheFilePath.toUri(), conf);
        InputStreamReader streamRD= new InputStreamReader(filesys.open(cacheFilePath));
        BufferedReader inputBuffer = new BufferedReader(streamRD)) {
        String lines;
        while ((lines = inputBuffer.readLine()) != null) {
          String[] splits = lines.split(",");
          if (splits.length >= 2) {
            if (Long.parseLong(splits[0])<=Long.parseLong(MaxValRep) && Long.parseLong(splits[1])<=Long.parseLong(MaxValRep)){
              broadcastMap.computeIfAbsent(splits[0], node -> new HashSet<>()).add(splits[1]);//
            }
          }
        }
      }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] split = value.toString().split(",");
      if (split.length == 2){
        long triangles=0;
        //using sets for unique values
        if (broadcastMap.containsKey(split[1])){
          //Traversing through HashMap to get the 3rd edge
          HashSet<String> pt3_values = broadcastMap.get(split[1]);
          for (String pt3: pt3_values){
            HashSet<String> pt1_check = broadcastMap.get(pt3);
            //Unlike RS, X-Y-X avoided
            if (pt1_check.contains(split[0])){
              triangles+=1;
              context.write(NullWritable.get(),new Text(split[0]+","+split[1]+","+pt3));
            }
          }
        }
        context.getCounter(Counters.Count3TrianglesRep).increment(triangles);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new Error("Check Erronous Arguments! Usage: <Input Path> <Output Path>");
    }
    try {
      ToolRunner.run(new repTriangleApprox(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }
}
