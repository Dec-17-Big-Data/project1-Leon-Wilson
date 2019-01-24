

import mapper.GenderAnalysisQ5Mapper;
import reducer.GenderAnalysisQ5Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author Leon Wilson
 * 
 *Question: What was the percent increase in female vulnerable employment from
 *
 *Though process:
 * For question 5 I wanted to stick with the employment theme of the project so I decided to track
 * vulnerable employment throughout the years 
 * 
 *Approach:
 * First I parsed out all records that didn't have the indicator code SL.EMP.VULN.FE.ZS and country code USA
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
 * In post processing I graphed the data onto a scatter plot to show the increase and decrease through the years.
 * 
 *Assumptions:
 * I assumed that there were at least 44 columns in each field
 * I assumed that the high and low year wouldn't be the same
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
	    job.setJobName("Gender Statistic Analysis Q5");
	    
	    
	    //confusion starts
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(GenderAnalysisQ5Mapper.class);
	    job.setReducerClass(GenderAnalysisQ5Reducer.class);
	    
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
