

import static org.junit.Assert.assertTrue;
import mapper.GenderAnalysisQ3Mapper;
import reducer.GenderAnalysisQ3Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class Q3MRUnitTest {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	private Text input = new Text("United States\",\"USA\",\"Employment to population ratio, 15+, male (%) (national estimate)\",\"SL.EMP.TOTL.SP.MA.NE.ZS\",\"78.879997253418\",\"77.5599975585938\",\"77.7300033569336\",\"77.0999984741211\",\"77.2699966430664\",\"77.5100021362305\",\"77.8600006103516\",\"77.9599990844727\",\"77.8000030517578\",\"77.6100006103516\",\"76.1800003051758\",\"74.9000015258789\",\"75.0299987792969\",\"75.5500030517578\",\"74.879997253418\",\"71.7300033569336\",\"72.0400009155273\",\"72.7799987792969\",\"73.7600021362305\",\"73.8399963378906\",\"72.0199966430664\",\"71.2900009155273\",\"69.0199966430664\",\"68.8099975585938\",\"70.6800003051758\",\"70.9000015258789\",\"70.9700012207031\",\"71.4700012207031\",\"72.0199966430664\",\"72.4599990844727\",\"72.0400009155273\",\"70.3600006103516\",\"69.8399963378906\",\"70.0199966430664\",\"70.4300003051758\",\"70.7900009155273\",\"70.9000015258789\",\"71.3099975585938\",\"71.5800018310547\",\"71.6500015258789\",\"71.8899993896484\",\"70.870002746582\",\"69.7099990844727\",\"68.9000015258789\",\"69.1900024414063\",\"69.5999984741211\",\"70.0699996948242\",\"69.7600021362305\",\"68.5\",\"64.5500030517578\",\"63.689998626709\",\"63.8699989318848\",\"64.3899993896484\",\"64.4000015258789\",\"64.879997253418\",\"65.3399963378906\",\"65.7699966430664\",");
	private Text inputMissing = new Text("United States\",\"USA\",\"Employment to population ratio, 15+, male (%) (national estimate)\",\"SL.EMP.TOTL.SP.MA.NE.ZS\",\"78.879997253418\",\"77.5599975585938\",\"77.7300033569336\",\"77.0999984741211\",\"77.2699966430664\",\"77.5100021362305\",\"77.8600006103516\",\"77.9599990844727\",\"77.8000030517578\",\"77.6100006103516\",\"76.1800003051758\",\"74.9000015258789\",\"75.0299987792969\",\"75.5500030517578\",\"74.879997253418\",\"71.7300033569336\",\"72.0400009155273\",\"72.7799987792969\",\"73.7600021362305\",\"73.8399963378906\",\"72.0199966430664\",\"71.2900009155273\",\"69.0199966430664\",\"68.8099975585938\",\"70.6800003051758\",\"70.9000015258789\",\"70.9700012207031\",\"71.4700012207031\",\"72.0199966430664\",\"72.4599990844727\",\"72.0400009155273\",\"70.3600006103516\",\"69.8399963378906\",\"70.0199966430664\",\"70.4300003051758\",\"70.7900009155273\",\"70.9000015258789\",\"71.3099975585938\",\"71.5800018310547\",\"71.6500015258789\",\"71.8899993896484\",\"70.870002746582\",\"69.7099990844727\",\"68.9000015258789\",\"69.1900024414063\",\"69.5999984741211\",\"70.0699996948242\",\"69.7600021362305\",\"68.5\",\"64.5500030517578\",\"63.689998626709\",\"63.8699989318848\",\"\",\"64.4000015258789\",\"64.879997253418\",\"65.3399963378906\",\"65.7699966430664\",");

	private Text mapperKey = new Text("United States Employment to population ratio, 15+, male (%) (national estimate)");
	private Text reducerKey = new Text("United States Employment to population ratio, 15+, male (%) (national estimate) from 2000 to 2016");
	
	private Text mapperOUTPUT = new Text(
		"2000,71.8899993896484;"
		+ "2001,70.870002746582;"
		+ "2002,69.7099990844727;"
		+ "2003,68.9000015258789;"
		+ "2004,69.1900024414063;"
		+ "2005,69.5999984741211;"
		+ "2006,70.0699996948242;"
		+ "2007,69.7600021362305;"
		+ "2008,68.5;"
		+ "2009,64.5500030517578;"
		+ "2010,63.689998626709;"
		+ "2011,63.8699989318848;"
		+ "2012,64.3899993896484;"
		+ "2013,64.4000015258789;"
		+ "2014,64.879997253418;"
		+ "2015,65.3399963378906;"
		+ "2016,65.7699966430664");
	
	private Text mapperOUTPUTmissing = new Text(
		"2000,71.8899993896484;"
		+ "2001,70.870002746582;"
		+ "2002,69.7099990844727;"
		+ "2003,68.9000015258789;"
		+ "2004,69.1900024414063;"
		+ "2005,69.5999984741211;"
		+ "2006,70.0699996948242;"
		+ "2007,69.7600021362305;"
		+ "2008,68.5;"
		+ "2009,64.5500030517578;"
		+ "2010,63.689998626709;"
		+ "2011,63.8699989318848;"
		+ "2012,N/A;"
		+ "2013,64.4000015258789;"
		+ "2014,64.879997253418;"
		+ "2015,65.3399963378906;"
		+ "2016,65.7699966430664");
	
	private Text reducerOUTPUT = new Text(
		"-1.4\t"
		+ "-1.65\t"
		+ "-1.14\t"
		+ "0.42\t"
		+ "0.57\t"
		+ "0.67\t"
		+ "-0.42\t"
		+ "-1.8\t"
		+ "-5.76\t"
		+ "-1.34\t"
		+ "0.28\t"
		+ "0.81\t"
		+ "0.03\t"
		+ "0.72\t"
		+ "0.7\t"
		+ "0.65\t"
		+ "-0.53\t"
		+ "71.88\t"
		+ "65.76");
	
	private Text reducerOUTPUTMissing = new Text(
		"-1.4\t"
		+ "-1.65\t"
		+ "-1.14\t"
		+ "0.42\t"
		+ "0.57\t"
		+ "0.67\t"
		+ "-0.42\t"
		+ "-1.8\t"
		+ "-5.76\t"
		+ "-1.34\t"
		+ "0.28\t"
		+ "None\t"
		+ "None\t"
		+ "0.72\t"
		+ "0.7\t"
		+ "0.65\t"
		+ "-0.53\t"
		+ "71.88\t"
		+ "65.76");
	
	
	@Before
	public void setUp(){
		GenderAnalysisQ3Mapper mapper = new GenderAnalysisQ3Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		GenderAnalysisQ3Reducer reducer = new GenderAnalysisQ3Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	//TEST MapperDriver
	@Test
	public void testMapper(){
		List<Pair<Text,Text>> mapperResults;
		
		Pair<Text,Text> testPair = new Pair<Text, Text>(mapperKey, mapperOUTPUT);
		mapDriver.withInput(new LongWritable(1),input);

		try{
			mapperResults = mapDriver.run();
			assertTrue(mapperResults.get(0).compareTo(testPair) == 0);
		} catch (IOException e){
			
		}
	}
	
	@Test
	public void testMapperMissingData(){
		List<Pair<Text,Text>> mapperMissingResults;
		
		Pair<Text,Text> testPair = new Pair<Text, Text>(mapperKey, mapperOUTPUTmissing);
		mapDriver.withInput(new LongWritable(1),inputMissing);

		try{
			mapperMissingResults = mapDriver.run();
			assertTrue(mapperMissingResults.get(0).compareTo(testPair) == 0);
		} catch (IOException e){
			
		}
	}
	
	@Test
	public void testReducer(){
		List<Pair<Text,Text>> reducerResults;
		Pair<Text,Text> testPair = new Pair<Text, Text>(reducerKey, reducerOUTPUT);
		List<Text> dataList = new ArrayList<Text>();
		dataList.add(mapperOUTPUT);
		
		reduceDriver.withInput(mapperKey, dataList);
		
		
		try{
			reducerResults = reduceDriver.run();
			assertTrue(reducerResults.get(0).compareTo(testPair) == 0);
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	//TEST ReducerDriver
	@Test
	public void testReducerMissingData(){
		List<Pair<Text,Text>> reducerMissingResults;
		Pair<Text,Text> testPair = new Pair<Text, Text>(reducerKey, reducerOUTPUTMissing);
		List<Text> dataList = new ArrayList<Text>();
		dataList.add(mapperOUTPUTmissing);
		
		reduceDriver.withInput(mapperKey, dataList);
		
		
		try{
			reducerMissingResults = reduceDriver.run();
			assertTrue(reducerMissingResults.get(0).compareTo(testPair) == 0);
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	//TEST MapReduceDriver
	@Test
	public void testMapReduce(){
		List<Pair<Text,Text>> mapReduceResults;
		Pair<Text,Text> testPair = new Pair<Text, Text>(reducerKey, reducerOUTPUT);
		
		mapReduceDriver.withInput(new LongWritable(1),input);
		
		try{
			mapReduceResults = mapReduceDriver.run();
			assertTrue(mapReduceResults.get(0).compareTo(testPair) == 0);
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	@Test
	public void testMapReduceMissingData(){
		List<Pair<Text,Text>> mapReduceResults;
		Pair<Text,Text> testPair = new Pair<Text, Text>(reducerKey, reducerOUTPUTMissing);
		
		mapReduceDriver.withInput(new LongWritable(1),inputMissing);
		
		try{
			mapReduceResults = mapReduceDriver.run();
			assertTrue(mapReduceResults.get(0).compareTo(testPair) == 0);
		} catch (IOException e){
			e.printStackTrace();
		}
	}
}
