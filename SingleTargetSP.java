package comp9313.ass2;

import java.io.IOException;
import java.util.StringTokenizer;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

import java.util.Set;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SingleTargetSP {
	//initial counter, we will use this counter in the second mapreduce function
	public static enum counter_task{
		TIME;
	}
	public static String OUT = "output";
	public static String IN = "input";
	//create the first mapreduce function
	//The first mapreduce function is aim to read the data from the input file.
	//In order to solve this problem, we could treat it as the SSSP problem by inverse the arrows
	//by the given graph, then it becomes the problem SSSP and for the output path we also inverse the path
	//to get the final result.
	public static class ReadMapper extends Mapper<Object,Text,Text,Text>{
		private HashMap<String,String> map = new HashMap<String,String>();
		//we use Hash map function to store and iterate
		//the format is (node_id, adj_node:distance|...)

		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line,"\n");
			while(tokens.hasMoreTokens()){
				String zbw = tokens.nextToken();
				StringTokenizer st = new StringTokenizer(zbw);
				st.nextToken();
				String nid = st.nextToken();
				String to_id = st.nextToken();
				String distance = st.nextToken();
				String adjID = "";
				//original file:
				//EdgeID FromNodeID ToNodeID Distance
				//here in order to inverse the arrow we set the to_id as the key
				if(map.containsKey(to_id)==true){
					adjID = nid+":"+distance;
					map.put(to_id, adjID+"|"+map.get(to_id));
				}else{
					adjID = nid+":"+distance;
					map.put(to_id, adjID+"|");
				}
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			String node = "";
			String source_node = context.getConfiguration().get("source_node");
			Set<String> se = map.keySet();
			Iterator<String> it = se.iterator();
			double zero = 0.0;
			double infinite = Double.MAX_VALUE;
			//for initial iteration, we set the distance zero for the source_node to itself
			//we set others infinite from source_node to others
			//we use max_value to represent infinite
			//it is convenient to compare
			while(it.hasNext()){
				node = it.next();
				if(source_node.compareTo(node)==0){
					context.write(new Text(node), new Text(zero+"\t"+map.get(node)));
				}else{
					context.write(new Text(node), new Text(infinite+"\t"+map.get(node)));
				}
			}
		}
	}
	public static class ReadReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			String result = "";
			String source_node = context.getConfiguration().get("source_node");
			for(Text val:values){
				result+=val;
			}
			result += "\t";
			result += source_node;
			//In the reduce step, we set the last element to source_node
			//this is convenient for us to record the path for the next mapreduce function
			//the out put format is like
			//nodeid (0 or MAX) adjacent_link source_node
			context.write(new Text(key), new Text(result));
			
		}
	}

//This is the second mapreduce function, and we use counter and while loop to control this step.
//According the Pseudo-code for shortest path in our Chapter5 PPT lecture notes,
//we do a slightly changed and then use it to find the shortest path
	public static class STMapper extends Mapper<Object,Text,LongWritable,Text>{
		@Override
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			Text getline = new Text();
			String line = value.toString();
			String[] token = line.split("\t");
			int node_id = Integer.parseInt(token[0]);//node id
			double distance = Double.parseDouble(token[1]);//get the distance
			String connection = token[2].toString();//the adjacent_link

			String[] dis = connection.split("\\|");

			for(String str: dis){
				if(str.equalsIgnoreCase("UNMODED")){
					continue;// we don't need to deal the unchanged node
				}
				String[] detail = str.split(":");
				int adj = Integer.parseInt(detail[0]);// from node
				double adj_dis = Double.parseDouble(detail[1]);//to node
				double sum_dis;//sum the distance
				if (distance != Double.MAX_VALUE){
					sum_dis = distance+adj_dis;
					getline.set("VALUE"+" "+sum_dis+" "+adj+"->"+token[3]);
					//in this step we record the path
					//when we finish the sum we know this node can be reached
					//format:VALUE sum_diatance path

					context.write(new LongWritable(adj), getline);
					getline.clear();
				}
			}
			//pass the current node distance
			//foramt: VALUE current_distance path
			getline.set("VALUE"+" "+token[1]+" "+token[3]);
			context.write(new LongWritable(node_id), getline);
			getline.clear();
			//pass the adj_list and determine the counter for the reduce
			//format: NODES current_distance list
			getline.set("NODES"+" "+token[1]+" "+token[2]);
			context.write(new LongWritable(node_id),getline);

			getline.clear();
			
			
		}
	}
	public static class STReducer extends Reducer<LongWritable,Text,LongWritable,Text>{
		@Override
		public void reduce(LongWritable key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
			Text word = new Text();
			String path="";
			double stop_distance = Double.MAX_VALUE;//start at infinite
			double short_distance = Double.MAX_VALUE;//start at infinite
			String un_list = "UNMODED";
			
			for (Text val:values){
				String[] sp = val.toString().split(" ");
				if (sp[0].equalsIgnoreCase("NODES")){
					stop_distance = Double.parseDouble(sp[1]);
					un_list = sp[2];
				}
				else if(sp[0].equalsIgnoreCase("VALUE")){

					//compare the shortest distance
					if(Double.parseDouble(sp[1]) <= short_distance){
						short_distance = Double.parseDouble(sp[1]);;
						path = sp[2];
					}
				}
			}
			//the format like: node distance adj_list path
			word.set(short_distance+"\t"+un_list+"\t"+path);


			context.write(key, word);

			
			word.clear();
			//determine the counter value whether to terminate iteration
			if (short_distance<stop_distance){
				context.getCounter(counter_task.TIME).setValue(1);
			}
			
		}
	}
	//This is the last mapreduce function.
	//It aims to output the correct format as required.
	//Input format: NodeID Distance Adj_list Path
	public static class ResultMapper extends Mapper<Object,Text,IntWritable,Text>{
		private IntWritable nodeID = new IntWritable();
		private Text path = new Text();
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line,"\n");
			while(tokens.hasMoreTokens()){
				String zbw = tokens.nextToken();
				StringTokenizer st = new StringTokenizer(zbw);
				int node = Integer.valueOf(st.nextToken());
				String distance = st.nextToken();
				st.nextToken();
				String pathway = st.nextToken();
				nodeID.set(node);
				path.set(distance+"\t"+pathway);
				//here according to the requirement of PDF
				//we only output the reachable result
				if(Double.parseDouble(distance)!=Double.MAX_VALUE){
					context.write(nodeID, path);
				}

				
			}
		}
	}
	//final output format: SourceNodeID Distance Path
	public static class ResultReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
		private Text result = new Text();
		public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
			for(Text value:values){
				result.set(value);
				context.write(key, result);
				//output the result 
				//in nodeID order
			}
		}
	}
	public static void main(String[] args) throws Exception{
		IN = args[0];//input
		OUT= args[1];//output
		String source_node = args[2];//query node
		int timer = 0;
		String input = IN;
		String output = OUT + timer;

		Configuration conf = new Configuration();
		conf.set("source_node", source_node);
		Job job = Job.getInstance(conf,"first map");
		job.setJarByClass(SingleTargetSP.class);
		job.setMapperClass(ReadMapper.class);
		job.setReducerClass(ReadReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
		//finish first map function
		input = output;
		timer += 1;
		output = OUT + timer;
		//start the second map function
		//use while loop to control

		boolean isdone = false;
		while(isdone==false){


			Configuration conf1 = new Configuration();
			Job job2 = Job.getInstance(conf1, "second map");
			job2.setJarByClass(SingleTargetSP.class);
			job2.setMapperClass(STMapper.class);
			job2.setReducerClass(STReducer.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path(input));
			FileOutputFormat.setOutputPath(job2, new Path(output));
			job2.waitForCompletion(true);
			FileSystem hdfs = FileSystem.get(new URI(OUT),conf1);
			//we delete the temporary file
			Path tmp_path = new Path(input);
			hdfs.deleteOnExit(tmp_path);

			//we use counter to judge the termination
			Counters counters = job2.getCounters();
			Counter counter = counters.findCounter(counter_task.TIME);

			if(counter.getValue()==0){
				isdone = true;
			}
			
			timer+=1;

			input = output;

			output = OUT+timer;
		}
		//the last mapreduce function to output result
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3,"last map");
		job3.setJarByClass(SingleTargetSP.class);
		job3.setMapperClass(ResultMapper.class);
		job3.setReducerClass(ResultReducer.class);
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(input));
		FileOutputFormat.setOutputPath(job3, new Path(OUT));
		job3.waitForCompletion(true);
		//we also need to deal with the last temporary file
		FileSystem hdfs = FileSystem.get(new URI(OUT),conf3);
		Path tmp_path = new Path(input);
		hdfs.deleteOnExit(tmp_path);
		System.exit(0);
		
	}
	
}
