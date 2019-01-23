

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapper.GenderAnalysisQ5Mapper;
import reducer.GenderAnalysisQ5Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class Q5MRUnitTest {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	private Text input = new Text("United States\",\"USA\",\"Vulnerable employment, female (% of female employment)\",\"SL.EMP.VULN.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"6.46000003814698\",\"6.66999995708466\",\"6.64999997615815\",\"6.73000001907348\",\"6.74000012874604\",\"6.76000022888183\",\"7\",\"7.10000026226044\",\"6.97000020742416\",\"6.7600000500679\",\"6.49999994039535\",\"6.59999978542328\",\"6.7700001001358\",\"6.79999983310699\",\"6.73999977111816\",\"6.81999993324279\",\"6.37999987602234\",\"6.3799998164177\",\"7.07999987900257\",\"6.93000000715256\",\"6.86000007390976\",\"6.74000018835067\",\"6.43000021576881\",\"6.20999994874\",\"6.29999984800816\",\"6.2199999243021\",\"6.04999982565641\",\"6.12999999523163\",\"6.10000001639128\",\"5.94999992102384\",\"5.95999990403652\",\"5.8400000333786\",\"5.57000003755092\",\"5.65000016987324\",\"5.60000000149012\",\"5.52000007778407\",\"5.62000021338463\",\"5.5900002270937\",\"5.3899999409914\",\"5.28999981284142\",\"5.18000013381242\",");

	private Text mapperKey = new Text("United States Vulnerable employment, female (% of female employment)");
	private Text reducerKey = new Text("United States Vulnerable employment, female (% of female employment) from 2000 to 2016");
	
	private Text mapperOUTPUT = new Text(
			"2000,6.29999984800816;"
			+ "2001,6.2199999243021;"
			+ "2002,6.04999982565641;"
			+ "2003,6.12999999523163;"
			+ "2004,6.10000001639128;"
			+ "2005,5.94999992102384;"
			+ "2006,5.95999990403652;"
			+ "2007,5.8400000333786;"
			+ "2008,5.57000003755092;"
			+ "2009,5.65000016987324;"
			+ "2010,5.60000000149012;"
			+ "2011,5.52000007778407;"
			+ "2012,5.62000021338463;"
			+ "2013,5.5900002270937;"
			+ "2014,5.3899999409914;"
			+ "2015,5.28999981284142;"
			+ "2016,5.18000013381242");
	
	private Text reducerOUTPUT = new Text(
			"-1.27\t"
			+ "-2.73\t"
			+ "1.32\t"
			+ "-0.32\t"
			+ "-2.62\t"
			+ "0.16\t"
			+ "-1.84\t"
			+ "-4.62\t"
			+ "1.43\t"
			+ "-0.88\t"
			+ "-1.42\t"
			+ "1.81\t"
			+ "-0.53\t"
			+ "-3.75\t"
			+ "-1.85\t"
			+ "-1.89\t"
			+ "-1.1\t"
			+ "6.29\t"
			+ "5.18");
	
	
	@Before
	public void setUp(){
		GenderAnalysisQ5Mapper mapper = new GenderAnalysisQ5Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		GenderAnalysisQ5Reducer reducer = new GenderAnalysisQ5Reducer();
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
	
	//TEST ReducerDriver
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
}
