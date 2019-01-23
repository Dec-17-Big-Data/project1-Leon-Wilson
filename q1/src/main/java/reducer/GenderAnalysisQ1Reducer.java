package reducer;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenderAnalysisQ1Reducer extends Reducer<Text, Text, Text, DoubleWritable>  {
	 @Override
	  public void reduce(Text _key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException {
		  Text key = _key;
		  
		  for(Text t : values){
			  Double d = new Double(t.toString());
			  if(d > 30.0){
				  context.write(key, new DoubleWritable(d));
			  }
		  }
	  }
}
