package master2018.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VehicleTelematics {
	
	private static final String SPEED_RADAR = "/speedFines.csv";
	private static final String AVERAGE_SPEED_RADAR = "/avgSpeedFines.csv";
	private static final String ACCIDENT_REPORTER = "/accidents.csv";

	public static void main(String[] args) {
		
		// Set up the execution environment. DataStream API
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Set up the Time Characteristic
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Get input data paths from the 2 args that we have sent by the console (1:input and 1:output folder)
		String inFilePath = args[0];
		String outFolder = args[1];
		
		// Client Execution:
//		String outFolder = "/home/fran/probando2";
//		String inFilePath = "/home/fran/part1";
	
		
		/**
		 * Implements the string tokenizer that splits sentences into tuples as a user-defined
		 * FlatMapFunction. The function takes a line (String) and splits it into
		 * multiple tuples in the form of "(Time, VID, Spd, XWay, Lane, Dir, Seg, Pos)" (Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>).
		 */
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
		
		
		// We prepare the inFile data, firstly we apply readTextFile and then we map it
		SingleOutputStreamOperator<String> source = env.readTextFile(inFilePath).setParallelism(1);
		SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapSource = source.map(new GeneralMap());

		// We execute SpeedRadar and we write as csv in the first of the output paths
		DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> outSpeedRadar = SpeedRadar.detectSpeed(mapSource);
		outSpeedRadar.writeAsCsv(outFolder + SPEED_RADAR, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		// We execute SpeedRadar and we write as csv in the first of the output paths
		DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> outAverageSpeed = AverageSpeedRadar.detectAverageSpeed(mapSource);
		outAverageSpeed.writeAsCsv(outFolder + AVERAGE_SPEED_RADAR, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		// We execute SpeedRadar and we write as csv in the first of the output paths
		DataStream<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> outAccidentReporter = AccidentReporter.detectAccident(mapSource);
		outAccidentReporter.writeAsCsv(outFolder + ACCIDENT_REPORTER, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
		
		
		// Trigger the program execution
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
