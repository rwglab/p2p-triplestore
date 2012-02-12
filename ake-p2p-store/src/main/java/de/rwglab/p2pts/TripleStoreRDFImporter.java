package de.rwglab.p2pts;

import org.openrdf.model.Statement;
import org.openrdf.rio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;

public class TripleStoreRDFImporter implements RDFHandler {

	private static final Logger log = LoggerFactory.getLogger(TripleStoreRDFImporter.class);

	private HashMap<String, String> ns = new HashMap<String, String>();

	private final TripleStore tripleStore;

	public TripleStoreRDFImporter(TripleStore tripleStore) {
		super();
		this.tripleStore = tripleStore;
	}

	public void importRDF(File file) {
		String filename = file.getAbsolutePath();
		RDFParser parser = Rio.createParser(Rio.getParserFormatForFileName(filename, RDFFormat.NTRIPLES));

		try {
			Reader in;
			if (Charset.isSupported("UTF-8")) {
				in = new InputStreamReader(new FileInputStream(filename), "UTF-8");
			} else {
				in = new InputStreamReader(new FileInputStream(filename), Charset.defaultCharset());
			}
			parseFile(parser, in);
			in.close();
		} catch (FileNotFoundException e) {
			System.out.println("Could not find file: " + filename);
		} catch (UnsupportedEncodingException e) {

		} catch (IOException e) {
			System.out.println("Error while accessing stream");
		}
	}

	public void importRDF(URL url) {
		RDFParser parser = Rio.createParser(Rio.getParserFormatForFileName(url.toString(), RDFFormat.NTRIPLES));
		try {
			InputStreamReader in;
			if (Charset.isSupported("UTF-8")) {
				in = new InputStreamReader(url.openStream(),
						"UTF-8"
				);
			} else {
				in = new InputStreamReader(url.openStream(),
						Charset.defaultCharset()
				);
			}
			parseFile(parser, in);
			in.close();
		} catch (MalformedURLException e) {
			System.out.println("Malformed URI.");
		} catch (IOException e) {
			System.out.println("Errow while accessing stream.");
		}

	}

	private void parseFile(RDFParser p, Reader r) {
		try {
			p.setRDFHandler(this);
			p.parse(r, "");
		} catch (IOException e) {
			System.out.println("Errow while reading rdf file.");
		} catch (RDFParseException e) {
			System.out.println("Errow while parsing rdf file: " + e);
		} catch (RDFHandlerException e) {
			System.out.println("Errow while handling rdf file.");
		}
	}

	public void endRDF() throws RDFHandlerException {
	}

	public void handleComment(String c) throws RDFHandlerException {
	}

	public void handleNamespace(String prefix, String url)
			throws RDFHandlerException {
		ns.put(prefix + ":", url);
	}

	public void handleStatement(Statement s) throws RDFHandlerException {

		String sub = resolveNS(s.getSubject().stringValue());
		String pred = resolveNS(s.getPredicate().stringValue());
		String obj = resolveNS(s.getObject().stringValue());

		log.debug("{} - {} - {}", new Object[]{sub, pred, obj});

		tripleStore.insert(new Triple(sub, pred, obj));
	}

	private String resolveNS(String s) {
		for (String prefix : ns.keySet()) {
			if (s.contains(prefix)) {
				return s.replace(prefix, ns.get(prefix));
			}
		}
		return s;
	}

	public void startRDF() throws RDFHandlerException {
	}
}
