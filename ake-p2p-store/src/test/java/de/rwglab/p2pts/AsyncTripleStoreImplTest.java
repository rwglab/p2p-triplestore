package de.rwglab.p2pts;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;

public class AsyncTripleStoreImplTest {

	private ChordSetup chordSetup;

	private ChordBasedSetMapAsync<String, String> map;

	private TripleStore tripleStore;

	@Before
	public void setUp() throws Exception {
		chordSetup = new ChordSetup();
		chordSetup.setUp();
		map = new ChordBasedSetMapAsync<String, String>(
				chordSetup.getBootstrapDHashService().getDhasher(),
				chordSetup.getExecutor()
		);
		tripleStore = new TripleStoreImpl(map, chordSetup.getExecutor());
	}

	@After
	public void tearDown() throws Exception {
		chordSetup.tearDown();
	}

	@Test
	public void testTripleIsFoundWithUnknownObject() throws Exception {
		HashSet<Triple> expectedResultSet = newHashSet(new Triple("a", "b", "c"));
		Triple searchBy = new Triple("a", "b", null);
		testTripleIsFoundWithOneUnknownInternal(expectedResultSet, searchBy);
	}

	@Test
	public void testTripleIsFoundWithUnknownPredicate() throws Exception {
		HashSet<Triple> expectedResultSet = newHashSet(new Triple("a", "b", "c"));
		Triple searchBy = new Triple("a", null, "c");
		testTripleIsFoundWithOneUnknownInternal(expectedResultSet, searchBy);
	}

	@Test
	public void testTripleIsFoundWithUnknownSubject() throws Exception {
		HashSet<Triple> expectedResultSet = newHashSet(new Triple("a", "b", "c"));
		Triple searchBy = new Triple(null, "b", "c");
		testTripleIsFoundWithOneUnknownInternal(expectedResultSet, searchBy);
	}

	@Test
	public void testTwoTriplesAreFoundIfTwoMatchWithUnknownObject() throws Exception {
		HashSet<Triple> expectedResultSet = newHashSet(
				new Triple("a", "b", "c"),
				new Triple("a", "b", "d")
		);
		Triple searchBy = new Triple("a", "b", null);
		testTripleIsFoundWithOneUnknownInternal(expectedResultSet, searchBy);
	}

	@Test
	public void testTwoTriplesAreFoundIfTwoMatchWithUnknownPredicate() throws Exception {
		HashSet<Triple> expectedResultSet = newHashSet(
				new Triple("a", "b", "c"),
				new Triple("a", "d", "c")
		);
		Triple searchBy = new Triple("a", null, "c");
		testTripleIsFoundWithOneUnknownInternal(expectedResultSet, searchBy);
	}

	@Test
	public void testTwoTriplesAreFoundIfTwoMatchWithUnknownSubject() throws Exception {
		HashSet<Triple> expectedResultSet = newHashSet(
				new Triple("a", "b", "c"),
				new Triple("d", "b", "c")
		);
		Triple searchBy = new Triple(null, "b", "c");
		testTripleIsFoundWithOneUnknownInternal(expectedResultSet, searchBy);
	}

	@Test
	public void testIfDeletingTheOnlyTripleWorksWhenCheckingWithUnknownObject() throws Exception {

		tripleStore.insert(new Triple("a", "b", "c")).get();
		tripleStore.delete(new Triple("a", "b", "c")).get();

		assertEquals(Sets.<Triple>newHashSet(), tripleStore.get(new Triple("a", "b", null)).get());
	}

	@Test
	public void testIfDeletingTheOnlyTripleWorksWhenCheckingWithUnknownPredicate() throws Exception {

		tripleStore.insert(new Triple("a", "b", "c")).get();
		tripleStore.delete(new Triple("a", "b", "c")).get();

		assertEquals(Sets.<Triple>newHashSet(), tripleStore.get(new Triple("a", null, "c")).get());
	}

	@Test
	public void testIfDeletingTheOnlyTripleWorksWhenCheckingWithUnknownSubject() throws Exception {

		tripleStore.insert(new Triple("a", "b", "c")).get();
		tripleStore.delete(new Triple("a", "b", "c")).get();

		assertEquals(Sets.<Triple>newHashSet(), tripleStore.get(new Triple(null, "b", "c")).get());
	}

	@Test
	public void testIfUpdatingASingleEntryWorksForObject() throws Exception {

		Triple initialTriple = new Triple("a", "b", "c");
		Triple updatedTriple = new Triple("a", "b", "d");

		tripleStore.insert(initialTriple).get();
		tripleStore.update(initialTriple, updatedTriple).get();

		assertEquals(
				Sets.<Triple>newHashSet(updatedTriple),
				tripleStore.get(new Triple("a", "b", null)).get()
		);
	}

	@Test
	public void testIfUpdatingASingleEntryWorksForPredicate() throws Exception {

		Triple initialTriple = new Triple("a", "b", "c");
		Triple updatedTriple = new Triple("a", "d", "c");

		tripleStore.insert(initialTriple).get();
		tripleStore.update(initialTriple, updatedTriple).get();

		assertEquals(
				Sets.<Triple>newHashSet(updatedTriple),
				tripleStore.get(new Triple("a", null, "c")).get()
		);
	}

	@Test
	public void testIfUpdatingASingleEntryWorksForSubject() throws Exception {

		Triple initialTriple = new Triple("a", "b", "c");
		Triple updatedTriple = new Triple("d", "b", "c");

		tripleStore.insert(initialTriple).get();
		tripleStore.update(initialTriple, updatedTriple).get();

		assertEquals(
				Sets.<Triple>newHashSet(updatedTriple),
				tripleStore.get(new Triple(null, "b", "c")).get()
		);
	}

	@Test
	public void testIfUpdatingOneOfMultipleEntriesWorksForObject() throws Exception {

		Triple tripleToUpdate = new Triple("a", "b", "d");

		Triple otherTriple1 = new Triple("a", "b", "c");
		Triple otherTriple2 = new Triple("a", "b", "e");

		Triple updatedTriple = new Triple("a", "b", "f");

		insertTriples(newHashSet(otherTriple1, tripleToUpdate, otherTriple2));

		tripleStore.update(tripleToUpdate, updatedTriple).get();

		assertEquals(
				Sets.<Triple>newHashSet(updatedTriple, otherTriple1, otherTriple2),
				tripleStore.get(new Triple("a", "b", null)).get()
		);
	}

	@Test
	public void testIfUpdatingOneOfMultipleEntriesWorksForPredicate() throws Exception {

		Triple tripleToUpdate = new Triple("a", "b", "c");

		Triple otherTriple1 = new Triple("a", "d", "c");
		Triple otherTriple2 = new Triple("a", "e", "c");

		Triple updatedTriple = new Triple("a", "f", "c");

		insertTriples(newHashSet(otherTriple1, tripleToUpdate, otherTriple2));

		tripleStore.update(tripleToUpdate, updatedTriple).get();

		assertEquals(
				Sets.<Triple>newHashSet(updatedTriple, otherTriple1, otherTriple2),
				tripleStore.get(new Triple("a", null, "c")).get()
		);
	}

	@Test
	public void testIfUpdatingOneOfMultipleEntriesWorksForSubject() throws Exception {

		Triple tripleToUpdate = new Triple("a", "b", "c");

		Triple otherTriple1 = new Triple("d", "b", "c");
		Triple otherTriple2 = new Triple("e", "b", "c");

		Triple updatedTriple = new Triple("f", "b", "c");

		insertTriples(newHashSet(otherTriple1, tripleToUpdate, otherTriple2));

		tripleStore.update(tripleToUpdate, updatedTriple).get();

		assertEquals(
				Sets.<Triple>newHashSet(updatedTriple, otherTriple1, otherTriple2),
				tripleStore.get(new Triple(null, "b", "c")).get()
		);
	}

	@Test
	public void testIfConcatenationForHashingOfTwoValuesOfATripleDoesntBreakTheMapSemanticsForSP() throws Exception {

		Triple expectedTriple = new Triple("ab", "c", "d");
		Triple similarTriple = new Triple("a", "bc", "e");

		insertTriples(newHashSet(expectedTriple, similarTriple));

		Set<Triple> actualTriples = tripleStore.get(new Triple("ab", "c", null)).get();

		assertEquals(newHashSet(expectedTriple), actualTriples);
	}

	@Test
	public void testIfConcatenationForHashingOfTwoValuesOfATripleDoesntBreakTheMapSemanticsForSO() throws Exception {

		Triple expectedTriple = new Triple("a", "b", "cd");
		Triple similarTriple = new Triple("ac", "e", "d");

		insertTriples(newHashSet(expectedTriple, similarTriple));

		Set<Triple> actualTriples = tripleStore.get(new Triple("a", null, "cd")).get();

		assertEquals(newHashSet(expectedTriple), actualTriples);
	}

	@Test
	public void testIfConcatenationForHashingOfTwoValuesOfATripleDoesntBreakTheMapSemanticsForPO() throws Exception {

		Triple expectedTriple = new Triple("a", "bc", "d");
		Triple similarTriple = new Triple("e", "b", "cd");

		insertTriples(newHashSet(expectedTriple, similarTriple));

		Set<Triple> actualTriples = tripleStore.get(new Triple(null, "bc", "d")).get();

		assertEquals(newHashSet(expectedTriple), actualTriples);
	}

	@SuppressWarnings("unchecked")
	private void testTripleIsFoundWithOneUnknownInternal(Set<Triple> expectedResultSet, Triple searchBy)
			throws ExecutionException, InterruptedException {

		insertTriples(expectedResultSet);

		Set<Triple> actualResultSet = tripleStore.get(searchBy).get();

		assertEquals(expectedResultSet, actualResultSet);
	}

	private void insertTriples(final Set<Triple> triples) throws ExecutionException, InterruptedException {

		List<ListenableFuture<Void>> futures = newArrayList();

		// fork
		for (Triple triple : triples) {
			futures.add(tripleStore.insert(triple));
		}

		// join
		for (ListenableFuture<Void> future : futures) {
			future.get();
		}
	}
}
