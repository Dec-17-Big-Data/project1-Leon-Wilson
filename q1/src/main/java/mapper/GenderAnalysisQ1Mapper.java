package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenderAnalysisQ1Mapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		  
		  String line = value.toString();

		  String[] valueSplit= line.split("\",\"");
		  String[] indicators = valueSplit[3].split("\\.");
		  
		  StringBuilder outKey = new StringBuilder();
		  StringBuilder outValue = new StringBuilder();
		  
		  Integer sizeOF = valueSplit.length;
		  if(indicators[0].equals("SE") && indicators[2].equals("CMPL") && indicators[3].equals("FE"))
		  {
			  outKey.append(valueSplit[0] + " " + valueSplit[2]);
			  
			  for(int i = sizeOF - 1;i > sizeOF - 15 && i >= 0 ;i--){
				  try{
					  if(!valueSplit[i].equals("")){
						  Double.valueOf(valueSplit[i]);
						  outValue.append(valueSplit[i]);
						  break;
					  }
				  }catch (NumberFormatException e){
					  
				  }
			  }
			  if(!outValue.toString().equals("")){
				  context.write(new Text(outKey.toString()), new Text(outValue.toString()));
			  }
		  }

	  }	
}
