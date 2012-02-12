package de.rwglab.p2pts;

import com.google.inject.Inject;
import com.sun.jersey.core.util.Base64;
import de.rwglab.p2pts.dto.UpdateTriple;
import de.rwglab.p2pts.util.InjectLogger;
import org.slf4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Set;

@Path("/")
public class TripleStoreRestResource {

	public static class Base64ParseException extends WebApplicationException {

		public Base64ParseException() {
			super(Response
					.status(Response.Status.BAD_REQUEST)
					.entity("Failed to base64-decode one or more of the query parameters.")
					.build()
			);
		}
	}

	@InjectLogger
	private Logger log;

	@Inject
	private TripleStore tripleStore;

	@GET
	public Response get(@QueryParam("s") String s, @QueryParam("p") String p, @QueryParam("o") String o) {
		return getInternal(new Triple(s, p, o));
	}

	private Response getInternal(final Triple triple) {

		try {

			log.debug("get({})", triple);

			ValueList valueList = new ValueList();
			valueList.values = new ArrayList<String>();

			Set<Triple> triples = tripleStore.get(triple).get();

			if (triples == null) {
				return Response.ok(valueList).build();
			}

			for (Triple rdfTriple : triples) {
				if (triple.subject == null) {
					valueList.values.add(rdfTriple.subject);
				} else if (triple.predicate == null) {
					valueList.values.add(rdfTriple.predicate);
				} else {
					valueList.values.add(rdfTriple.object);
				}
			}

			return Response.ok(valueList).build();

		} catch (Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
	}

	@GET
	@Path("get")
	public Response getBase64(@QueryParam("s") String s, @QueryParam("p") String p, @QueryParam("o") String o) {
		return getInternal(parseBase64Triple(s, p, o));
	}

	@POST
	public Response update(final UpdateTriple updateTriple) {
		return updateInternal(updateTriple.oldTriple, updateTriple.newTriple);
	}

	@POST
	@Path("update")
	public Response updateBase64(@QueryParam("oldS") String oldS,
								 @QueryParam("newS") String newS,
								 @QueryParam("oldP") String oldP,
								 @QueryParam("newP") String newP,
								 @QueryParam("oldO") String oldO,
								 @QueryParam("newO") String newO) {

		return updateInternal(parseBase64Triple(oldS, oldP, oldO), parseBase64Triple(newS, newP, newO));
	}

	private Response updateInternal(final Triple oldTriple, final Triple newTriple) {

		try {

			log.debug("update(old={}, new={})", oldTriple, newTriple);

			tripleStore.update(oldTriple, newTriple).get();

			return Response.ok().build();

		} catch (Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
	}

	@PUT
	public Response insert(final Triple triple) {

		try {

			log.debug("insert({})", triple);

			tripleStore.insert(triple).get();

			return Response.ok().build();

		} catch (Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
	}

	@POST
	@Path("insert")
	public Response insertBase64(@QueryParam("s") String s,
								 @QueryParam("p") String p,
								 @QueryParam("o") String o) {

		return insert(parseBase64Triple(s, p, o));
	}

	@DELETE
	public Response delete(final Triple triple) {

		try {

			log.debug("delete({})", triple);

			tripleStore.delete(triple);

			return Response.ok().build();

		} catch (Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
	}

	@POST
	@Path("delete")
	public Response deleteBase64(@QueryParam("s") String s,
								 @QueryParam("p") String p,
								 @QueryParam("o") String o) {

		return delete(parseBase64Triple(s, p, o));
	}

	private Triple parseBase64Triple(final String s, final String p, final String o) {

		String subject;
		String predicate;
		String object;

		try {

			subject = s == null ? null : new String(Base64.decode(s));
			predicate = p == null ? null : new String(Base64.decode(p));
			object = o == null ? null : new String(Base64.decode(o));

		} catch (Exception e) {
			throw new Base64ParseException();
		}

		return new Triple(subject, predicate, object);
	}

}
