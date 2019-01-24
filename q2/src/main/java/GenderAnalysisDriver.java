
import mapper.GenderAnalysisQ2Mapper;
import reducer.GenderAnalysisQ2Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author Leon Wilson
 * 
 *Question: List the average increase in female education in the U.S from the year 2000
 * 
 *Thought process:
 * The thought process behind this question was that since it was such a broad question, it would be important to
 * show the change in more than one level of education. With this in mind, I decide to obtain all relevant data that
 * matched certain indicator codes. After that I can decide which data would best show the increase in female
 * education
 * 
 *Approach:
 * First I parsed through the data looking for all the fields with the educational (SE) and female (FE) indicators while
 * excluding results from gross graduation ratio (CMPL)
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
 * In post processing I filtered the results to show the percent change for female pupils & teachers throughout the
 * various levels of education
 * 
 *Assumptions:
 * I assumed the amount of rows is static by checking to see if the array has at least 44 element
 * I assumed that any data the matches the indicators could be relevant
 * I assumed that the indicators would return all the relevant matches
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
