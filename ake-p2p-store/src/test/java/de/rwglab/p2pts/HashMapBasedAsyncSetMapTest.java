package de.rwglab.p2pts;

import org.junit.Before;

public class HashMapBasedAsyncSetMapTest extends SetMapAsyncTest {

	@Before
	public void before() {
		super.before(new HashMapBasedSetMapAsync<String, String>());
	}
}
