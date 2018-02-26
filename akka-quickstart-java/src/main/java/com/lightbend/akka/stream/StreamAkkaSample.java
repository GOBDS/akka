package com.lightbend.akka.stream;

import java.math.BigInteger;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

public class StreamAkkaSample {

	public static void main(String[] args) {

		final Source<Integer, NotUsed> source = Source.range(1, 1000);

		final ActorSystem system = ActorSystem.create("QuickStart");
		final Materializer materializer = ActorMaterializer.create(system);

		final CompletionStage<Done> done = source.runForeach(i -> System.out.println(i), materializer);

		done.thenRun(() -> system.terminate());

		final Source<BigInteger, NotUsed> factorials = source.scan(BigInteger.ONE,
				(acc, next) -> acc.multiply(BigInteger.valueOf(next)));

		final CompletionStage<IOResult> result = factorials.map(BigInteger::toString).runWith(lineSink("factorial2.txt"), materializer);
		
		result.thenRun(() -> system.terminate());
	}

	public static Sink<String, CompletionStage<IOResult>> lineSink(String filename) {
		return Flow.of(String.class).map(s -> ByteString.fromString(s.toString() + "\n"))
				.toMat(FileIO.toPath(Paths.get(filename)), Keep.right());
	}
}

