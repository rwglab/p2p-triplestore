package de.rwglab.p2pts;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Set;

public interface TripleStore {

	ListenableFuture<Void> insert(Triple triple);

	ListenableFuture<Set<Triple>> get(Triple triple);

	ListenableFuture<Void> update(Triple oldTriple, Triple newTriple);

	ListenableFuture<Void> delete(Triple triple);
}
