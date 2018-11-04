package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class SpeedRadar {
	
	
		//
	    // Stream Operators
	    //
	
	    static SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> detectSpeed(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> data) {
	        return data
	                .filter(new MaxSpeedFilter())
	                .map(new MapSpeedRadar()); 
	        }

		//
		// User Functions
		//
		
		/**
         *The function takes a DataStream of Tuple8 and transforms them into Tuple6
		 *in the form of "(Time, VID, XWay, Seg, Dir, Spd)".
		 */
		static class MapSpeedRadar implements MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> {
			public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception { 
				
				Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(in.f0, in.f1, in.f3, in.f6, in.f5, in.f2 );

				return out;
			}
		}
		
		/**
		 * Filters the cars with a speed higher than 90Mph as a user-defined
		 * FlatMapFunction. The function takes a Tuple8 and applies the predicate value.f5>90
		 * and keeps the elements that return true, and discard the false. 
		 */
		static class MaxSpeedFilter implements FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>{

			@Override
			public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value)
					throws Exception {
				if (value.f2>90) {
					return true;
				} else {
					return false;
				}
				//return (value.f2>90);
			}}


}
