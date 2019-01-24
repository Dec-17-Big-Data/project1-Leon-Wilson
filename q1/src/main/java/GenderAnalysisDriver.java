

import mapper.GenderAnalysisQ1Mapper;
import reducer.GenderAnalysisQ1Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author Leon Wilson
 *
 *Question: Identify the countries where the percentage of female graduates is less than 30%
 *
 *Thought process:
 * The thought process behind this question was to  simply extract the relevant data (graduation rate) and check if it was
 * below 30%
 *
 *Approach:
 * The first step was to extract the data from the file that contained female gross graduation rate from each country
 * and find the latest data it had on the subject up to a threshold of 15 years prior.
 * 
 * If the data existed then I would check to see if it was lower than 30% and, if so, map it to my mapper output
 *
 * After I mapped the data, I converted it to a DoubleWritable and stored that value in my output
 *
 *Assumptions:
 * I assumed that countries without data in the last 15 columns (years) wouldn't be necessary to include because the data 
 * for their graduation rates could potentially be  vastly different than their current statistics
 */
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
	    job.setJobName("Gender Statistic Analysis Q1");
	    
	    
	    //confusion starts
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(GenderAnalysisQ1Mapper.class);
	    job.setReducerClass(GenderAnalysisQ1Reducer.class);
	    
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
