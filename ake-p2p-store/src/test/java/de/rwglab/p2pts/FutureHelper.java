package de.rwglab.p2pts;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.common.collect.Lists.newArrayList;

public class FutureHelper {

	public static <V> List<V> awaitFutures(Future<V>... futures) throws ExecutionException, InterruptedException {
		List<V> list = newArrayList();
		for (Future<V> future : futures) {
			list.add(future.get());
		}
		return list;
	}

}
