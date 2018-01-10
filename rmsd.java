public class rmsd{

	//实现map
	public class Map extends Mapper <>{
		public void map(){

		}

	}

	//实现reduce
	public class Reduce extends Reducer<>{

	}

	//实现combine
	public class Combine extends Combiner<>{

	}

	//实现main
	public void public static void main(String[] args) {
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker","master:9000");

		String[] ioArgs=new String[]{"rmsd_in","rmsd_out"};
		String[] otherArgs=new  GenericOptionsParser(conf,ioArgs).getRemainingArgs();

		if(otherArgs.length!=2){
			System.err.println("error");
			System.exit(2);
		}

		//设置配置信息
		Job job = new Job();

		job.setJarByClass("rmsd.class");				//指定处理程序
		job.setMapperClass("Map.class");				//指定map处理类
		job.setReducerClass("Reduce.class");			//指定reduce处理类
		job.setCombbinerClass("Combine.class");			//指定combine处理类

		//设置输出类型
		job.setOutputKeyClass("Text.class");
		job.setOutputValueClass("Text.class");

		//设置输入输出目录
		FileInputFormat.addInutPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true)?0:1);
	}

}