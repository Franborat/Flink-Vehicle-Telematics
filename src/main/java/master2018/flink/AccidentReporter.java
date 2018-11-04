package master2018.flink;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class AccidentReporter {

	//
	// User Functions
	//

	
	/**
	 * Filters the cars with a speed of value "0" as a user-defined
	 * FlatMapFunction. The function takes a Tuple8 and applies the predicate value.f2==0
	 * and keeps the elements that return true, and discard the false. 
	 */
	static class NullSpeedFilter implements FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>{

		@Override
		public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value)
				throws Exception {
			if (value.f2==0) {
				return true;
			} else {
				return false;
			}
			//return (value.f2>90);
		}}
	
	
	static class AccidentReporterWindowFunction implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow>{

		@Override
		public void apply(Tuple key, GlobalWindow window,
				Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
				Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out)
				throws Exception {
			
			//We create an iterator over the input elements 
			Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
			
			//We extract the initial stopping time
			Integer initialTime = iterator.next().f0;
			//Counter
			int i=1;
			//Last tuple of the window
			Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> endTuple = null;
			while(iterator.hasNext()) {
				i++;
				endTuple = iterator.next();
			}
			
			if(i==4) {
				
				// System.out.println(initialTime+" "+endTuple.f0+" "+endTuple.f1+" "+endTuple.f3+" "+endTuple.f6+" "+endTuple.f5+" "+endTuple.f7);
				
				out.collect(new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>
								(initialTime, endTuple.f0, endTuple.f1, endTuple.f3, endTuple.f6, endTuple.f5, endTuple.f7));
			}
			
		}}

	
    static SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> detectAccident(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> data) {
        return data
                .filter(new NullSpeedFilter())
                .keyBy(1,3,5,6,7)
                .countWindow(4, 1)
                .apply(new AccidentReporterWindowFunction()); 
        }	
    
}
