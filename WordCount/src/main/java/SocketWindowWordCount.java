package main.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


@SuppressWarnings("serial")
public class SocketWindowWordCount {

	public static void main(String[] args) throws Exception {

		// the host and the port to connect to
		final String hostname;
		final int port;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args); //reads the arguments
			
			hostname = params.has("hostname") ? params.get("hostname") : "localhost";
			//@ Tushar:  here we are providing port as  9000 coz we started the program on 9000(netcat) -> nc -l 9000
			port = params.getInt("port"); 
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'SocketWindowWordCount " +
				"--hostname <hostname> --port <port>', where hostname (localhost by default) " +
				"and port is the address of the text server");
			System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
				"type the input text into the command line");
			return;
		}

		// get the execution environment
		// @Tushar: providing the context in which the flink program will execute
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data by connecting to the socket 
		//@Tushar:  this is nothing but to 
			// -create Datastream /Datset object
			//provide a source, here we are creating a socket text stream here
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");

		/*@Tushar: Data Transformations
			 - parsing the data, 
				- group it, window it, and aggregate the counts
		*/
		DataStream<WordWithCount> windowCounts = text
				.flatMap(new FlatMapFunction<String, WordWithCount>() {  //@Tushar --> flatMap: Takes one element and produces zero, one, or more elements. A flatmap function that splits sentences to words
					@Override
					public void flatMap(String value, Collector<WordWithCount> out) {
						for (String word : value.split("\\s")) {
							System.out.println("value of word is :::::::::::::::::::::::" + word);
							out.collect(new WordWithCount(word, 1L));
						}
					}
				})
				/*@tushar: Logically partitions a stream into disjoint partitions. 
				All records with the same key are assigned to the same partition. Internally, keyBy() is implemented with hash partitioning*/
				.keyBy("word")
				/*Tushar:  Window is applicable when u r working on stream and u have to perform some aggregate functions, here we are usng window based on time (not data also known as element) */
				.timeWindow(Time.seconds(5))

				.reduce(new ReduceFunction<WordWithCount>() {
					@Override
					/* @Tushar: A "rolling" reduce on a keyed data stream. 
					 * Combines the current element with the last reduced value and emits the new value. 
					 */
					public WordWithCount reduce(WordWithCount a, WordWithCount b) {
						return new WordWithCount(a.word, a.count + b.count);
					}
				});

		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);

		/*@Tushar: Invoke the sink
		 * All Flink programs are executed lazily, execute() will invoke the data loading as well as the transformations. 
		 */
		env.execute("Socket Window WordCount");
	}

	// ------------------------------------------------------------------------

	/**
	 * Data type for words with count.
	 */
	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}
}
