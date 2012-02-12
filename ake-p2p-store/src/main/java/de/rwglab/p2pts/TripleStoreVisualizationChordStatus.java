package de.rwglab.p2pts;

import java.math.BigInteger;

public class TripleStoreVisualizationChordStatus {

	public BigInteger min;

	public BigInteger max;

	public BigInteger id;

	public String hostname;

	public int triples;

	public long redirects;

	public long gets;

	public long puts;

	@Override
	public String toString() {
		return "{\n"
				+ "\"type\":\"status\",\n"
				+ "\"timestamp\":" + System.currentTimeMillis() + ",\n"
				+ "\"min\":" + min + ",\n"
				+ "\"max\":" + max + ",\n"
				+ "\"id\":" + id + ",\n"
				+ "\"hostname\":\"" + hostname + "\",\n"
				+ "\"triples\":" + triples + ",\n"
				+ "\"redirects\":" + redirects + ",\n"
				+ "\"gets\":" + gets + ",\n"
				+ "\"puts\":" + puts + "\n"
				+ "}";
	}
}