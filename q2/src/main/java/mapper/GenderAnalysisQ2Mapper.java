package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenderAnalysisQ2Mapper extends
		Mapper<LongWritable, Text, Text, Text> {
	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		  
		  String line = value.toString();
		  String[] valueSplit= line.split("\",\"");
		  String[] indicators = valueSplit[3].split("\\.");
		  String countryCode = valueSplit[1];
		  
		  StringBuilder outKey = new StringBuilder();
		  StringBuilder outValue = new StringBuilder();
		  Integer year = 2000;
		  
		  if(indicators[0].equals("SE") && !indicators[2].equals("CMPL") && indicators[indicators.length - 2].equals("FE")){
			  if(countryCode.equals("USA")){
				  outKey.append(valueSplit[0] + " " + valueSplit[2]);
					  for(int i = 44; i < valueSplit.length ; i++){
						  try{
							  if(!valueSplit[i].equals("")&& !Double.isNaN(Double.valueOf(valueSplit[i].replace("\",", "")))){
								  outValue.append(year + "," +valueSplit[i].replace("\",", ""));
							  } else {
								  outValue.append(year + ",N/A");
							  }
						  }catch (NumberFormatException e){
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

	  }}
