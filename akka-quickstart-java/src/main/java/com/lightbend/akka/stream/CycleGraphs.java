package com.lightbend.akka.stream;

import java.util.Arrays;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ClosedShape;
import akka.stream.FanInShape2;
import akka.stream.FlowShape;
import akka.stream.Materializer;
import akka.stream.Outlet;
import akka.stream.OverflowStrategy;
import akka.stream.SinkShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Concat;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.ZipWith;

public class CycleGraphs {
	public static void main(String[] args) {
		final ActorSystem system = ActorSystem.create("QuickStart");
		final Materializer materializer = ActorMaterializer.create(system);
		
		final Source<Integer, NotUsed> in = Source.from(Arrays.asList(1, 2, 3, 4, 5));
		
		final Flow<Integer, Integer, NotUsed> printFlow =
				  Flow.of(Integer.class).map(s -> {
				    System.out.println(s);
				    return s;
				  });
		
		RunnableGraph.fromGraph(GraphDSL.create(b -> {
			  final UniformFanInShape<Integer, Integer> merge = b.add(Merge.create(2));
			  final UniformFanOutShape<Integer, Integer> bcast = b.add(Broadcast.create(2));
			  final FlowShape<Integer, Integer> droppyFlow = b.add(
			    Flow.of(Integer.class).buffer(10, OverflowStrategy.dropHead()));
			  final Outlet<Integer> src = b.add(in).out();
			  final FlowShape<Integer, Integer> printer = b.add(printFlow);
			  final SinkShape<Integer> ignore = b.add(Sink.ignore());
			  
			  b.from(src).viaFanIn(merge).via(printer).viaFanOut(bcast).to(ignore);
			               b.to(merge).via(droppyFlow).fromFanOut(bcast);
			  return ClosedShape.getInstance();
			})).run(materializer);
		
		RunnableGraph.fromGraph(GraphDSL.create(b -> {
			  final FanInShape2<Integer, Integer, Integer> zip =
			    b.add(ZipWith.create((Integer left, Integer right) -> left));
			  final UniformFanOutShape<Integer, Integer> bcast = b.add(Broadcast.create(2));
			  final UniformFanInShape<Integer, Integer> concat = b.add(Concat.create());
			  final FlowShape<Integer, Integer> printer = b.add(printFlow);
			  final SinkShape<Integer> ignore = b.add(Sink.ignore());

			  b.from(b.add(in)).toInlet(zip.in0());
			  b.from(zip.out()).via(printer).viaFanOut(bcast).to(ignore);
			    b.to(zip.in1()).viaFanIn(concat).from(b.add(Source.single(1)));
			                        b.to(concat).fromFanOut(bcast);
			  return ClosedShape.getInstance();
			}));
	}
}
