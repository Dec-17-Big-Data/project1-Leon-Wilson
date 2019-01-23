import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mapper.GenderAnalysisQ1Mapper;
import reducer.GenderAnalysisQ1Reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


public class Q1MRUnitTest {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	
	private Text input = new Text("United States\",\"USA\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.85857\",\"37.8298\",\"37.43131\",\"38.22037\",\"39.18913\",\"39.84185\",\"40.23865\",\"41.26198\",\"42.00725\",\"42.78946\",\"43.68347\",\"\",\"46.37914\",\"47.68032\",\"\",\"\",\"\",\"");
	private Text inputNoResult = new Text("Arab World\",\"ARB\",\"Gross graduation ratio, primary, female (%)\",\"SE.PRM.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"");

	private Text mapperKey = new Text("United States Gross graduation ratio, tertiary, female (%)");
	private Text mapperOUTPUT = new Text("47.68032");
	
	private Text reducerKey = new Text("United States Gross graduation ratio, tertiary, female (%)");
	private DoubleWritable reducerOUTPUT = new DoubleWritable(new Double("47.68032"));
	
	@Before
	public void setUp(){
		GenderAnalysisQ1Mapper mapper = new GenderAnalysisQ1Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		
		GenderAnalysisQ1Reducer reducer = new GenderAnalysisQ1Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
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
	public void testMapperNoResult(){
		List<Pair<Text,Text>> mapperResults;
		
		mapDriver.withInput(new LongWritable(1),inputNoResult);

		try{
			mapperResults = mapDriver.run();
			assertTrue(mapperResults.isEmpty());
		} catch (IOException e){
			
		}
	}
	
	//TEST ReducerDriver
	@Test
	public void testReducer(){
		List<Pair<Text,DoubleWritable>> reducerResults;
		Pair<Text,DoubleWritable> testPair = new Pair<Text, DoubleWritable>(reducerKey, reducerOUTPUT);
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
		List<Pair<Text,DoubleWritable>> mapReduceResults;
		Pair<Text,DoubleWritable> testPair = new Pair<Text, DoubleWritable>(reducerKey, reducerOUTPUT);
		
		mapReduceDriver.withInput(new LongWritable(1),input);
		
		try{
			mapReduceResults = mapReduceDriver.run();
			assertTrue(mapReduceResults.get(0).compareTo(testPair) == 0);
		} catch (IOException e){
			e.printStackTrace();
		}
	}
}
