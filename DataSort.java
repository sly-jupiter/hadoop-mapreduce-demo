package intern;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * 设计思路：
 * 对输入的数据进行排序，可以利用MapReduce过程本身带有的排序
 * MapReduce默认排序规则：
 * 		按照key值进行排序，
 * 		1：当key为IntWritable类型时，MapReduce将会按照数字的大小对key进行排序。
 * 		2：当key为String的Text类型时，MapReduce将会按照字典顺序对key字符串进行排序。
 */

public class DataSort {

	//map类中将输入的数据转换为IntWritable类型，再作为key值输出
	public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
		private static IntWritable data=new IntWritable();
		//实现map()
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line=value.toString();
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable());
		}
	}
	
	//Reduce类，将输入中的key复制到输出数据上的key中
	public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		
		//linenum统计当前key的位次
		private static IntWritable linenum=new IntWritable(1);
		public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			//根据输入的value：list中元素的个数决定key的输出次数
			for(IntWritable val:values){
				context.write(linenum, key);
				linenum=new IntWritable(linenum.get()+1);
			}
		}
	}
	
	public static void main (String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker", "master:9000/");
		
		String[] ioArgs=new String[]{"rmsd_in","rmsd_out"};
		String[] otherArgs=new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		
		if(otherArgs.length!=2){
			System.err.println("error");
			System.exit(2);
		}

		//
		Job job=new Job(conf,"RMSD");
		
		job.setJarByClass(RMSD.class);
		//设置map处理类
		job.setMapperClass(Map.class);
		//设置reduce处理类
		//job.setReducerClass(Reduce.class);
		//设置输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置输入输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
