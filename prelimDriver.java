package indexer;

import indexer.prelimMapReduce.Map;
import indexer.prelimMapReduce.Reduce;

import java.io.File;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class prelimDriver extends Configured implements Tool {
//driver for prelimMap Reduce
	
	
	//delete a file
	private static void deleteDir(File dir){
		System.out.println();
		  File[] contents = dir.listFiles();
		  for(File file: contents){
			  if(file.isDirectory())
				  deleteDir(file);
			  else
				  file.delete();
		  }
		  dir.delete();
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf());
	    job.setJarByClass(getClass());
	    job.setJobName(getClass().getSimpleName());
	    
	    
	    //set i/o info
	    FileInputFormat.addInputPath(job,new Path( "in/in1"));
	    FileOutputFormat.setOutputPath(job, new Path("out7/in"));
	 
	    job.setMapperClass(prelimMapReduce.Map.class);
	    job.setCombinerClass(prelimMapReduce.Reduce.class);
	    job.setReducerClass(prelimMapReduce.Reduce.class);
	 
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	 
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		File out = new File("out");
		if(out.exists())
			deleteDir(out);
		prelimDriver driver = new prelimDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
		}

}
