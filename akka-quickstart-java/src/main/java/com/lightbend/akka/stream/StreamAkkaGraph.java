package com.lightbend.akka.stream;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ClosedShape;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.Outlet;
import akka.stream.SinkShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.MergePreferred;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.UnzipWith;
import akka.util.ByteString;

public class StreamAkkaGraph {

	public static void main(String[] args) 
	{
		final ActorSystem system = ActorSystem.create("QuickStart");
		final Materializer materializer = ActorMaterializer.create(system);
		
		final Source<Integer, NotUsed> in = Source.from(Arrays.asList(1, 2, 3, 4, 5));
		final Source<Integer, NotUsed> in_1 = Source.from(Arrays.asList(6, 7, 8, 9, 10));
		final Source<Integer, NotUsed> in_2 = Source.from(Arrays.asList(11, 12, 13, 14, 15));
		final Source<Integer, NotUsed> in_3 = Source.from(Arrays.asList(16, 17, 18, 19, 20));
		final Source<Integer, NotUsed> in_4 = Source.from(Arrays.asList(21, 22, 23, 24, 25));
		final Source<Integer, NotUsed> in_5 = Source.from(Arrays.asList(26, 27, 28, 29, 30));
		final Source<Integer, NotUsed> in_6 = Source.from(Arrays.asList(31, 32, 33, 34, 35));

		// console output sink 
	    Sink<String, CompletionStage<Done>> sink = Sink.<String>foreach(System.out::println);
	    
		//final Sink<Object, CompletionStage<Done>> sink = Sink.head().foreach(a -> System.out.println(a));
	    
		final Flow<Integer, Integer, NotUsed> f1 = Flow.of(Integer.class).map(elem -> elem + 10);
		final Flow<Integer, Integer, NotUsed> f2 = Flow.of(Integer.class).map(elem -> elem + 20);
		final Flow<Integer, String, NotUsed> f3 = Flow.of(Integer.class).map(elem -> elem.toString());
		final Flow<Integer, Integer, NotUsed> f4 = Flow.of(Integer.class).map(elem -> elem + 30);
		
		final Flow<Integer, Boolean, NotUsed> f5 = Flow.of(Integer.class).map(elem -> {
			if(elem > 2) {
				return false;
			}else {
				return true;
			}
		});
		
		final RunnableGraph<CompletionStage<Done>> result =
				  RunnableGraph.fromGraph(
				    GraphDSL     // create() function binds sink, out which is sink's out port and builder DSL
				      .create(   // we need to reference out's shape in the builder DSL below (in to() function)
				        sink,                // previously created sink (Sink)
				        (builder, out) -> {  // variables: builder (GraphDSL.Builder) and out (SinkShape)
				          final UniformFanOutShape<Integer, Integer> bcast = builder.add(Broadcast.create(2));
				          final UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(2));
				          final UniformFanInShape<Integer, Integer> mergeSource = builder.add(MergePreferred.create(8));
				          //final MergePreferredShape<Integer> merge2 = builder.add(MergePreferred.create(1));
				          //final UniformFanOutShape<Integer, Integer> balance = builder.add(Balance.create(2));

				          //Define sources
				          final Outlet<Integer> source = builder.add(in).out();
				          final Outlet<Integer> source_1 = builder.add(in_1).out();
				          final Outlet<Integer> source_2 = builder.add(in_2).out();
				          final Outlet<Integer> source_3 = builder.add(in_3).out();
				          final Outlet<Integer> source_4 = builder.add(in_4).out();
				          final Outlet<Integer> source_5 = builder.add(in_5).out();
				          final Outlet<Integer> source_6 = builder.add(in_6).out();
				          
				          //Merge Sources
				          builder.from(source).toFanIn(mergeSource);
				          builder.from(source_1).toFanIn(mergeSource);
				          builder.from(source_2).toFanIn(mergeSource);
				          builder.from(source_3).toFanIn(mergeSource);
				          builder.from(source_4).toFanIn(mergeSource);
				          builder.from(source_5).toFanIn(mergeSource);
				          builder.from(source_6).toFanIn(mergeSource);
				          
				          builder.from(mergeSource).via(builder.add(f1))
				            .viaFanOut(bcast).via(builder.add(f2)).viaFanIn(merge)
				            .via(builder.add(f3)).to(out);  // to() expects a SinkS
				          builder.from(bcast).via(builder.add(f4)).toFanIn(merge);
				          return ClosedShape.getInstance();
				        }));
		
		RunnableGraph.fromGraph(result).run(materializer);
	}
}
