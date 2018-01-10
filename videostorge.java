
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class videostorge {
	
	public static void main( String args[]) throws IOException{
		
		Configuration conf =new Configuration();
		
		FileSystem hdfs_fs= FileSystem.get(conf);
		FileSystem local_fs= FileSystem.getLocal(conf);
		
		//set the path of data that you need to upload the videos
		Path localpath=new Path("/home/zkpk/Desktop/interndata");
		
		//set the path of hdfs to receive the videos you upload
		Path hdfspath=new Path("/video/");
		hdfs_fs.mkdirs(hdfspath);
		FileStatus[] inputFiles=local_fs.listStatus(localpath);
		
		FSDataOutputStream out;
		
		//write the video data to hdfs with a "for" 
		for(int i=0;i< inputFiles.length;i++){
			System.out.println(inputFiles[i].getPath().getName());
			
			FSDataInputStream in= local_fs.open(inputFiles [i].getPath());
			
			out=hdfs_fs.create(new Path("/video/"+inputFiles [i].getPath().getName()));
			
			byte[] buffer=new byte[256];
			int bytesRead=0;
			
			
			while((bytesRead=in.read(buffer))>0){
				out.write(buffer,0,bytesRead);
			}
			
			out.close();
			in.close();
			File file=new File(inputFiles[i].getPath().toString());
			
			file.delete();
			
		}
}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}