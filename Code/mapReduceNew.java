import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mapReduceNew {

  public static class dMapper
       extends Mapper<Object, Text, Text, Text>{

      //private final static IntWritable one = new IntWritable(1);
    private Text outVal = new Text();
      private Text outKey = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String inline = value.toString();
        if (!inline.startsWith("#"))
        {
            String [] inVals = inline.split("\t");
            outKey.set(inVals[0]);
            outVal.set(inVals[1]);
            context.write(outKey, outVal);
        }
    }
  }

  public static class dReducer
      extends Reducer<Text,Text,Text, Text> {
    private int max = 0;
    private int min = Integer.MAX_VALUE;
    private HashMap<String, Integer> hmap = new HashMap<String, Integer>();
    private Text longestAdjList = new Text();
    private Text maxSet = new Text();
    private Text minSet = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int cntr = 0;
      String adjlst = "";
      String maxNode = "";
      for (Text val : values)
      {
          adjlst = adjlst+","+val;
          cntr++;
      }
      adjlst = adjlst.substring(1);
      hmap.put(key.toString(), cntr);
      if(cntr > max)
      {
        max = cntr;
        maxSet.set(Integer.toString(max));
	adjlst = key.toString() + " : " + adjlst; 
        longestAdjList.set(adjlst);
      }
      if(cntr < min)
      {
        min = cntr;
        minSet.set(Integer.toString(min));
      }
      }
      public void cleanup(Context context) throws IOException, InterruptedException
      {
        List<String>  maxConnectivityNodes = new ArrayList<>();
        List<String>  minConnectivityNodes = new ArrayList<>();
        int count_max = 0, count_min = 0;
        for(Map.Entry<String, Integer> entry : hmap.entrySet()){
                if(entry.getValue() == max){
                        maxConnectivityNodes.add(entry.getKey());
                        count_max++;
                }
                if(entry.getValue() == min){
                        minConnectivityNodes.add(entry.getKey());
                        count_min++;
                }
        }
        context.write(new Text("Longest Adjacency list in Directed Graph"),longestAdjList);
        context.write(new Text("Maximun connectivity in Directed Graph"),maxSet);
        context.write(new Text("Minimum connectivity in Directed Graph"),minSet);
        context.write(new Text("Number of nodes with maximum connectivity in Directed Graph"), new Text(Integer.toString(count_max)));
        context.write(new Text("Number of nodes with minimum connectivity in Directed Graph"), new Text(Integer.toString(count_min)));
        context.write(new Text("Nodes with maximun connectivity in Directed Graph"),new Text(maxConnectivityNodes.toString()));
        context.write(new Text("Nodes with minimum connectivity Graph in Directed Graph"),new Text(minConnectivityNodes.toString()));
      }
  }

  public static class uMapper extends Mapper<Object, Text, Text, Text>{
     private Text outVal = new Text();
      private Text outKey = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String inline = value.toString();
        if (!inline.startsWith("#"))
        {
            String [] inVals = inline.split("\t");
            outKey.set(inVals[0]);
            outVal.set(inVals[1]);
            context.write(outKey, outVal);
            context.write(outVal, outKey);
        }
    }
  }

   public static class uReducer
      extends Reducer<Text,Text,Text, Text> {
    private int max = 0;
    private int min = Integer.MAX_VALUE;
    private Text maxSet = new Text();
    private Text minSet = new Text();
    private Text longestAdjList = new Text();
    private HashMap<String, Integer> hmap = new HashMap<String, Integer>();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int cntr = 0;
      String adjlst = "";
      HashSet<String> existingPairs = new HashSet<>();
      for (Text val : values)
      {
          if(existingPairs.contains(val.toString()))
            continue;
	  existingPairs.add(val.toString());
          adjlst = adjlst+","+val;
          cntr++;
      }
      hmap.put(key.toString(), cntr);
      adjlst = adjlst.substring(1);
        if(cntr > max)
      {
        max = cntr;
        maxSet.set(Integer.toString(cntr));
	adjlst = key.toString() + " : " + adjlst;
        longestAdjList.set(adjlst);
      }
      if(cntr < min)
      {
        min = cntr;
        minSet.set(Integer.toString(cntr));
      }
      }
      public void cleanup(Context context) throws IOException, InterruptedException
      {
        List<String>  maxConnectivityNodes = new ArrayList<>();
        List<String>  minConnectivityNodes = new ArrayList<>();
        int count_max = 0, count_min = 0;
        for(Map.Entry<String, Integer> entry : hmap.entrySet()){
                if(entry.getValue() == max){
                        maxConnectivityNodes.add(entry.getKey());
                        count_max++;
                        }
                if(entry.getValue() == min){
                        minConnectivityNodes.add(entry.getKey());
                        count_min++;
                        }
        }
        context.write(new Text("Longest Adjacency list in Undirected Graph"),longestAdjList);
        context.write(new Text("Maximun connectivity in Undirected Graph"),maxSet);
        context.write(new Text("Minimum connectivity in Undirected Graph"),minSet);
        context.write(new Text("Number of nodes with maximum connectivity in Undirected Graph"), new Text(Integer.toString(count_max)));
        context.write(new Text("Number of nodes with minimum connectivity in Undirected Graph"), new Text(Integer.toString(count_min)));
        context.write(new Text("Nodes with maximum connectivity in Undirected Graph"),new Text(maxConnectivityNodes.toString()));
        context.write(new Text("Nodes with minimum connectivity in Undriected Graph"),new Text(minConnectivityNodes.toString()));
      }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Directed Graph");
    job.setJarByClass(mapReduceNew.class);
    job.setMapperClass(dMapper.class);
    //job.setCombinerClass(dReducer.class);
    job.setReducerClass(dReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Undirected Graph");
    job2.setJarByClass(mapReduceNew.class);
    job2.setMapperClass(uMapper.class);
    //job2.setCombinerClass(uReducer.class);
    job2.setReducerClass(uReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
