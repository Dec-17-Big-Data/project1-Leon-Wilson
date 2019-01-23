
import mapper.GenderAnalysisQ2Mapper;
import reducer.GenderAnalysisQ2Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenderAnalysisDriver {
	public static void main(String[] args) throws Exception {

	    /*
	     * Validate that two arguments were passed from the command line.
	     */
	    if (args.length != 2) {
	      System.out.printf("Usage: GenderAnalysisDriver <input dir> <output dir>\n");
	      System.exit(-1);
	    }

	    /*
	     * Instantiate a Job object for your job's configuration. 
	     */
	    Job job = new Job();
	    
	    /*
	     * Specify the jar file that contains your driver, mapper, and reducer.
	     * Hadoop will transfer this jar file to nodes in your cluster running 
	     * mapper and reducer tasks.
	     */
	    job.setJarByClass(GenderAnalysisDriver.class);
	    
	    /*
	     * Specify an easily-decipherable name for the job.
	     * This job name will appear in reports and logs.
	     */
	    job.setJobName("Gender Statistic Analysis Q2");
	    
	    
	    //confusion starts
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(GenderAnalysisQ2Mapper.class);
	    job.setReducerClass(GenderAnalysisQ2Reducer.class);
	    
	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	  }
}
