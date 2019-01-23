package reducer;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenderAnalysisQ2Reducer extends Reducer<Text, Text, Text, Text>  {
	@Override
	  public void reduce(Text _key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException {
		
		DecimalFormat d = new DecimalFormat("#.##");
		d.setRoundingMode(RoundingMode.DOWN);
		  
		  for (Text value : values) {
				
				String[] byYear = value.toString().split(";");
				StringBuilder val = new StringBuilder();

				String firstYear = byYear[0].split(",")[0];
				String lastYear = byYear[byYear.length - 1].split(",")[0];

				StringBuilder key = new StringBuilder(_key.toString() + " from "
						+ firstYear + " to " + lastYear);

				for (int i = 0; i < byYear.length - 1; i++) {
					try {
						Double oldValue = Double.valueOf(d.format(Double.valueOf(byYear[i].split(",")[1])));
						Double newValue = Double.valueOf(d.format(Double.valueOf(byYear[i + 1].split(",")[1])));

						Double diff = newValue - oldValue;
						Double percentChange = (diff / oldValue) * 100;

						val.append(d.format(percentChange));
						val.append(",");
					} catch (NumberFormatException e) {
						val.append("None");
						val.append(",");
					} catch (Exception e){
						e.printStackTrace();
					}
				}
				Integer lowIndex = null;
				Integer highIndex = null;
				
				for(int low = 0, high = byYear.length-1;low < high;low++,high--){
					if(!byYear[low].split(",")[1].equals("N/A") && lowIndex == null){
						lowIndex = low;
					}
					
					if(!byYear[high].split(",")[1].equals("N/A") && highIndex == null){
						highIndex = high;
					}
				}
				
				if(highIndex != null && lowIndex != null){
					try {
						Double oldValue = Double.valueOf(d.format(Double.valueOf(byYear[lowIndex].split(",")[1])));
						Double newValue = Double.valueOf(d.format(Double.valueOf(byYear[highIndex].split(",")[1])));

						Double diff = newValue - oldValue;
						diff = diff / (highIndex - lowIndex);
						Double percentChange = (diff / oldValue) * 100;

						val.append(d.format(percentChange));
						val.append(","+oldValue+","+newValue);
					} catch (NumberFormatException e) {
						val.append("None");
						val.append(",");
					} catch (Exception e){
						e.printStackTrace();
					}
				}
				
				context.write(new Text(key.toString()), new Text(val.toString()));
			}
	}
}
