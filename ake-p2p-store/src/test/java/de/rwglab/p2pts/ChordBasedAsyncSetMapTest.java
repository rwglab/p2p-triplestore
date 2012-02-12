package de.rwglab.p2pts;

import org.junit.After;
import org.junit.Before;

public class ChordBasedAsyncSetMapTest extends SetMapAsyncTest {

	private ChordSetup chordSetup;

	@Before
	public void setUp() throws Exception {
		chordSetup = new ChordSetup();
		chordSetup.setUp();
		super.before(
				new ChordBasedSetMapAsync<String, String>(
						chordSetup.getBootstrapDHashService().getDhasher(),
						chordSetup.getExecutor()
				)
		);
	}

	@After
	public void tearDown() throws Exception {
		chordSetup.tearDown();
	}
}
