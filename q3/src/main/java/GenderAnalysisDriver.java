

import mapper.GenderAnalysisQ3Mapper;
import reducer.GenderAnalysisQ3Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author Leon Wilson
 *
 *Question: List the percent change in male employment from the year 2000
 * 
 *Thought process:
 * The thought process for this question was to find the male employment rate for each country and follow a similar
 * approach to question 2
 * 
 *Approach:
 * First I parsed through the data looking for the employment to population ratio for each country
 * 
 * Then I went through each of the columns of the fields starting at the year 2000 (44) and added their value to the end
 * of the value output string (format: year,value;)
 * 
 * Next I performed the percent change function on each year (((newYear - oldYear)/oldYear) * 100)) and stored that
 * value in the reducers output.
 * 
 * Finally, after going through all of the percent change functions for each year difference, I perform one last
 * percent change function between the earliest year with data and the last year with data (((newYear - oldYear)/oldYear)/yearDifference * 100)) 
 * and store that alongside the two values used at the end of the reducer output.
 * 
 * In post processing I removed any records that had an excess amount of empty spaces in their countries output and
 * 
 *Assumptions:
 *I assumed that there were at least 44 columns in each field
 *I assumed that the high and low year wouldn't be the same
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
	    job.setJobName("Gender Statistic Analysis Q3");
	    
	    
	    //confusion starts
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(GenderAnalysisQ3Mapper.class);
	    job.setReducerClass(GenderAnalysisQ3Reducer.class);
	    
	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    //job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	  }
}
