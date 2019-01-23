package mapper;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenderAnalysisQ4Mapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		  
		  String line = value.toString();
		  String lookFor = "SL.EMP.TOTL.SP.FE.NE.ZS";

		  String[] valueSplit= line.split("\",\"");
		  StringBuilder outKey = new StringBuilder();
		  StringBuilder outValue = new StringBuilder();
		  
		  Integer year = 2000;
		  //
		  if(valueSplit[3].equals(lookFor))
		  {
			  outKey.append(valueSplit[0] + " " + valueSplit[2]);
			  
			  for(int i = 44; i < valueSplit.length ; i++){
				  if(!valueSplit[i].equals("")){
					  outValue.append(year + "," +valueSplit[i]);
				  } else {
					  outValue.append(year + ",N/A");
				  }
				  year += 1;
				  if(i + 1 != valueSplit.length){
					  outValue.append(";");
				  }
			  }
			  context.write(new Text(outKey.toString()), new Text(outValue.toString()));
		  }

	  }
}
