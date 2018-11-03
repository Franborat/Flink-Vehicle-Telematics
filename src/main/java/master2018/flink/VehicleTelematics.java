package master2018.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class VehicleTelematics {

	public static void main(String[] args) {
		
		// Set up the execution environment. DataStream API
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Set up the Time Characteristic
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		// Get input data paths from the 2 args that we have sent by the console (input and output file)
		String inFilePath = args[0];
		
		
		class GeneralMap implements MapFunction<String, Tuple8<Integer, Integer,Integer, Integer, Integer, Integer, Integer, Integer>> {

			@Override
			public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception { 
				String[] fieldArray = in.split(",");
				Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>
				(Integer.parseInt(fieldArray[0]), Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[2]), Integer.parseInt(fieldArray[3]),
				Integer.parseInt(fieldArray[4]), Integer.parseInt(fieldArray[5]), Integer.parseInt(fieldArray[6]), Integer.parseInt(fieldArray[7]));

				return out;
			}
		}
		
		/**
		 * A timestamp assigner and watermark generator for streams where timestamps are monotonously ascending as a user-defined
		 * AscendingTimestampExtractor.The class overrides the abstract method extractAscendingTimestamp, assigning it to element.fo, refered to Time
		 */
		class MyTimestamp extends AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>{

			@Override
			public long extractAscendingTimestamp(
					Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {

				return element.f0;
			}}
		
		
		// We prepare the inFile data, firstly we apply readTextFile and then we map it
		SingleOutputStreamOperator<String> source = env.readTextFile(inFilePath).setParallelism(1);
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = source.map(new GeneralMap());

		
		
		// Trigger the program execution
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
