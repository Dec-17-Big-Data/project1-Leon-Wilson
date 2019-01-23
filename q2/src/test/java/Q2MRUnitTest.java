import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapper.GenderAnalysisQ2Mapper;
import reducer.GenderAnalysisQ2Reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


public class Q2MRUnitTest {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	
	private Text input = new Text("United States\",\"USA\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\",\"");
	
	private Text mapperKey = new Text("United States Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)");
	private Text reducerKey = new Text("United States Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%) from 2000 to 2015");
	
	private Text mapperOUTPUT = new Text(
			"2000,N/A;"
			+ "2001,N/A;"
			+ "2002,N/A;"
			+ "2003,N/A;"
			+ "2004,35.37453;"
			+ "2005,36.00504;"
			+ "2006,37.52263;"
			+ "2007,N/A;"
			+ "2008,38.44067;"
			+ "2009,39.15297;"
			+ "2010,39.89922;"
			+ "2011,40.53132;"
			+ "2012,41.12231;"
			+ "2013,20.18248;"
			+ "2014,20.38445;"
			+ "2015,20.68499");
	
	private Text reducerOUTPUT = new Text(
			"None,"
			+ "None,"
			+ "None,"
			+ "None,"
			+ "1.78,"
			+ "4.22,"
			+ "None,"
			+ "None,"
			+ "1.84,"
			+ "1.89,"
			+ "1.6,"
			+ "1.45,"
			+ "-50.92,"
			+ "0.99,"
			+ "1.47,"
			+ "-3.77,"
			+ "35.37,"
			+ "20.68");
	
	@Before
	public void setUp(){
		GenderAnalysisQ2Mapper mapper = new GenderAnalysisQ2Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		GenderAnalysisQ2Reducer reducer = new GenderAnalysisQ2Reducer();
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
