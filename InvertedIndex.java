package intern;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

 
public class InvertedIndex {
	
	public static class Map extends Mapper<Object, Text, Text, Text>{
		
		private Text keyinfo=new Text(); 	//word+url 
		private Text valueinfo=new Text();	//
		
		private FileSplit split;	//split
		
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			split=(FileSplit) context.getInputSplit();
			StringTokenizer itr=new StringTokenizer(value.toString());
			
			while(itr.hasMoreTokens()){
				keyinfo.set(itr.nextToken()+":"+split.getPath().toString());
				int splitIndex=split.getPath().toString().indexOf("file");
				keyinfo.set(itr.nextToken()+":"+split.getPath().toString().substring(splitIndex));
				
				valueinfo.set("1");
				
				context.write(keyinfo,valueinfo);
			}
		}
	}
	
	public static class Combine extends Reducer<Text,Text,Text,Text>{
		private Text info=new Text();
		
		public void reduce(Text key, Iterable <Text> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			
			for(Text value:values){
				sum += Integer.parseInt(value.toString());
			}
			
			int splitIndex=key.toString().indexOf(":");
			info.set(key.toString().substring(splitIndex+1)+":"+sum);
			key.set(key.toString().substring(0,splitIndex));
			
			context.write(key, info);
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		
		private Text result=new Text();
		
		public void reduce(Text key, Iterable <Text> values,Context context) throws IOException, InterruptedException{
			String filelist=new String();
			for(Text value:values){
				filelist += value.toString()+";";
			}
			result.set(filelist);
			context.write(key, result);
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker","master:9000");
		
		String[] ioArgs=new String[]{"index_in","index_out"};
		String[] otherArgs=new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		
		if(otherArgs.length!=2){
			System.err.println("error");
			System.exit(2);
		}
		
		Job job=new Job(conf, "inverted index");
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}






