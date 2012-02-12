package de.rwglab.p2pts;

import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static de.rwglab.p2pts.FutureHelper.awaitFutures;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public abstract class SetMapAsyncTest {

	private static final String KEY_1 = "key1";

	private static final String KEY_1_VALUE_1 = "key1_value1";

	private static final String KEY_1_VALUE_2 = "key1_value2";

	private static final String KEY_1_VALUE_3 = "key1_value3";

	private static final String KEY_2 = "key2";

	private static final String KEY_2_VALUE_1 = "key2_value1";

	private static final String KEY_2_VALUE_2 = "key2_value2";

	private static final String KEY_2_VALUE_3 = "key2_value3";

	protected SetMapAsync<String, String> mapAsync;

	protected void before(SetMapAsync<String, String> mapAsync) {
		this.mapAsync = mapAsync;
	}

	@Test
	public void testGetAfterInsertShouldReturnValue() throws Exception {

		mapAsync.put(KEY_1, KEY_1_VALUE_1, 2, TimeUnit.MINUTES).get();

		Set<String> set = mapAsync.get(KEY_1, 2, TimeUnit.MINUTES).get();

		assertEquals(1, set.size());
		assertTrue(set.contains(KEY_1_VALUE_1));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testGetAfterTwoInsertsShouldReturnTwoValues() throws Exception {

		awaitFutures(
				mapAsync.put(KEY_1, KEY_1_VALUE_1, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_1, KEY_1_VALUE_2, 2, TimeUnit.MINUTES)
		);

		Set<String> set = mapAsync.get(KEY_1, 2, TimeUnit.MINUTES).get();

		assertEquals(2, set.size());
		assertTrue(set.contains(KEY_1_VALUE_1));
		assertTrue(set.contains(KEY_1_VALUE_2));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testGetAfterThreeInsertsShouldReturnThreeValues() throws Exception {

		awaitFutures(
				mapAsync.put(KEY_1, KEY_1_VALUE_1, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_1, KEY_1_VALUE_2, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_1, KEY_1_VALUE_3, 2, TimeUnit.MINUTES)
		);

		Set<String> set = mapAsync.get(KEY_1, 2, TimeUnit.MINUTES).get();

		assertEquals(3, set.size());
		assertTrue(set.contains(KEY_1_VALUE_1));
		assertTrue(set.contains(KEY_1_VALUE_2));
		assertTrue(set.contains(KEY_1_VALUE_3));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testGetAfterRemoveOfOneOutOfOneShouldReturnEmptyValue() throws Exception {

		mapAsync.put(KEY_1, KEY_1_VALUE_1, 2, TimeUnit.MINUTES).get();

		mapAsync.remove(KEY_1, KEY_1_VALUE_1, 2, TimeUnit.MINUTES).get();

		assertTrue(mapAsync.get(KEY_1, 2, TimeUnit.MINUTES).get().isEmpty());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testGetAfterRemoveOfOneOutOfManyShouldReturnTheRest() throws Exception {

		awaitFutures(
				mapAsync.put(KEY_1, KEY_1_VALUE_1, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_1, KEY_1_VALUE_2, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_1, KEY_1_VALUE_3, 2, TimeUnit.MINUTES)
		);

		mapAsync.remove(KEY_1, KEY_1_VALUE_2, 2, TimeUnit.MINUTES).get();

		Set<String> set = mapAsync.get(KEY_1, 2, TimeUnit.MINUTES).get();

		assertEquals(2, set.size());
		assertTrue(set.contains(KEY_1_VALUE_1));
		assertTrue(set.contains(KEY_1_VALUE_3));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testGetAfterRemoveOfOneOutOfManyShouldReturnTheRestAlsoIfThereAreOtherKeys() throws Exception {

		awaitFutures(
				mapAsync.put(KEY_1, KEY_1_VALUE_1, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_1, KEY_1_VALUE_2, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_1, KEY_1_VALUE_3, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_2, KEY_2_VALUE_1, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_2, KEY_2_VALUE_2, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_2, KEY_2_VALUE_3, 2, TimeUnit.MINUTES)
		);

		mapAsync.remove(KEY_1, KEY_1_VALUE_2, 2, TimeUnit.MINUTES).get();

		Set<String> set = mapAsync.get(KEY_1, 2, TimeUnit.MINUTES).get();

		assertEquals(2, set.size());
		assertTrue(set.contains(KEY_1_VALUE_1));
		assertTrue(set.contains(KEY_1_VALUE_3));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testGetAfterInsertTheSameValueTwiceShouldReturnTheValueOnlyOnce() throws Exception {

		awaitFutures(
				mapAsync.put(KEY_1, KEY_1_VALUE_1, 2, TimeUnit.MINUTES),
				mapAsync.put(KEY_1, KEY_1_VALUE_1, 2, TimeUnit.MINUTES)
		);

		Set<String> set = mapAsync.get(KEY_1, 2, TimeUnit.MINUTES).get();

		assertEquals(1, set.size());
		assertTrue(set.contains(KEY_1_VALUE_1));
	}
}
