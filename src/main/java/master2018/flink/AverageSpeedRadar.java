package master2018.flink;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class AverageSpeedRadar {

	//
	// User Functions
	//

	/**
	 * A timestamp assigner and watermark generator for streams where timestamps are monotonously ascending as a user-defined
	 * AscendingTimestampExtractor.The class overrides the abstract method extractAscendingTimestamp, assigning it to element.fo, refered to Time
	 */
	static class MyTimestamp extends AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>{

		@Override
		public long extractAscendingTimestamp(
				Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {

			return element.f0*1000;
		}}

	/**
	 * Filters the cars with the segments between [52,56] as a user-defined
	 * FlatMapFunction. The function takes a Tuple8 and applies the predicate 52<=value.f6 && value.f6<=56
	 * and keeps the elements that return true, and discard the false. 
	 */
	static class SegmentFilter implements FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>{

		@Override
		public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value)
				throws Exception {
			if ( 52<=value.f6 && value.f6<=56 ) {
				return true;
			} else {
				return false;
			}
			//return (52<=value.f6 && value.f6<=56);
		}}
	
	/**
	 * Generates a Tuple3 key composed by (VID, XWay, Dir) 
	 */
	static class MyKeySelector implements KeySelector<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>{

		@Override
		public Tuple3<Integer, Integer, Integer> getKey(
				Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value) throws Exception {
			
			return new Tuple3<Integer, Integer, Integer>(value.f1, value.f3, value.f5);
		}}
	
	/**
	 * Detects the cars with an average speed higher than 60Mph during the seg[52,56] as a user-defined
	 * WindowFunction. A requisite is that the car passes through all of the 5 segments.
	 */
	static class AverageSpeedWindowFunction implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>, Tuple3<Integer, Integer, Integer>, TimeWindow>{

		@Override
		public void apply(Tuple3<Integer, Integer, Integer> key, TimeWindow window,
				Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
				Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out)
						throws Exception {

			//We create an iterator over the input elements 
			Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();

			// Final and initial time and position. MAX_VALUE and 0 are used as first value to compare with the first Tuple.
			// We use MAX_VALUE because we will keep the min value of all the tuples, and every value will be lower than that one.
			// We use 0 because we will keep the highest value of all the tuples, and every value will be higher than one.
			Integer t1 = Integer.MAX_VALUE;
			Integer p1 = Integer.MAX_VALUE;
			Integer t2 = 0;
			Integer p2 = 0;
			//Boolean array which will turn into true its values as the car passes through each segment
			boolean segments[] = {false, false, false, false, false};

			while(iterator.hasNext()) {

				Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple = iterator.next();

				t1 = Math.min(t1, tuple.f0);
				p1 = Math.min(p1, tuple.f7);
				t2 = Math.max(t2, tuple.f0);
				p2 = Math.max(p2, tuple.f7);

				int seg = tuple.f6;
				
				switch(seg) {
				case 52: segments[0] = true;
				System.out.print("------------------seg 52-------------------------");
				break;
				case 53: segments[1] = true;
				System.out.print("------------------seg 53-------------------------");
				break;
				case 54: segments[2] = true;
				System.out.print("------------------seg 54-------------------------");
				break;
				case 55: segments[3] = true;
				System.out.print("------------------seg 55-------------------------");
				break;
				case 56: segments[4] = true;
				System.out.print("------------------seg 56-------------------------");
				break;
				}
				System.out.println("");

			}

			if(segments[0]==true && segments[1]==true && segments[2]==true && segments[3]==true && segments[4]==true) {
				System.out.print("------------------seg [52-56] TRUE -------------------------");

				double averageSpeed = (Math.abs(Double.valueOf(p2) - Double.valueOf(p1)) / Math.abs(Double.valueOf(t2) - Double.valueOf(t1)))*2.23694 ;
				System.out.println("**********"+t1+","+t2+","+key.f0+","+key.f1+","+key.f2+","+averageSpeed+"***********");

				if(averageSpeed>60) {
					
					out.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>
					(t1, t2, key.f0, key.f1, key.f2, averageSpeed));
				}

			}}
		
	}

		static SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> detectAverageSpeed(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> data) {
			return data
					.filter(new SegmentFilter())
					.assignTimestampsAndWatermarks(new MyTimestamp())
					.keyBy(new MyKeySelector())
					.window(EventTimeSessionWindows.withGap(Time.milliseconds(30001)))
					.apply(new AverageSpeedWindowFunction()); 
		}




		
}
