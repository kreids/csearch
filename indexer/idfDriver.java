package indexer;

import java.io.File;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class idfDriver 
extends Configured implements Tool {

	
	
	// deletes a directory
	private static void deleteDir(File dir){
		System.out.println();
		  File[] contents = dir.listFiles();
		  //System.out.println(contents[0].toString());
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
	 
	    //set in/out
	    //FileInputFormat.addInputPath(job,new Path( "in/in1"));
	    FileInputFormat.addInputPath(job, new Path("out7/in"));
	    FileOutputFormat.setOutputPath(job, new Path("out/out1"));
	 
	    job.setMapperClass(idfMapReduce.Map.class);
	   // job.setCombinerClass(idfMapReduce.Reduce.class);
	    job.setReducerClass(idfMapReduce.Reduce.class);
	 
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	 
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		// BerkeleyDB.setEnvPath("Database");
		// db = BerkeleyDB.getInstance();
		// db.openDB();
		File out = new File("out/out1");
		if(out.exists())
			deleteDir(out);
		idfDriver driver = new idfDriver();
		//run idf driver
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
		}

}


