/*
 * This file is part of Chordless. Chordless is free software: you can redistribute it and/or modify it under the terms
 * of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details. You should have received a copy of the GNU General Public License along with
 * Chordless. If not, see <http://www.gnu.org/licenses/>. Copyright 2010 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.chordless.dhash.upgrade;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import cx.ath.troja.nja.Proxist;
import cx.ath.troja.nja.RuntimeArguments;

public class Upgrade {

	public static void main(String[] arguments) throws Exception {
		new Upgrade(new RuntimeArguments(arguments));
	}

	public static Object instance(ClassLoader loader, String className, Object... arguments) throws Exception {
		if (arguments.length == 0) {
			return loader.loadClass(className).newInstance();
		} else {
			Class klass = loader.loadClass(className);
			Constructor[] constructors = klass.getConstructors();
			for (int i = 0; i < constructors.length; i++) {
				Class[] parameterTypes = constructors[i].getParameterTypes();
				if (parameterTypes.length == arguments.length) {
					boolean matches = true;
					for (int j = 0; j < arguments.length; j++) {
						matches &= (Proxist.samePrimitive(parameterTypes[j], arguments[j].getClass())
								|| (!parameterTypes[j].isPrimitive() && arguments[j] == null) || parameterTypes[j].isInstance(arguments[j]));
					}
					if (matches) {
						return constructors[i].newInstance(arguments);
					}
				}
			}
			return null;
		}
	}

	public static void println(String s) {
		// System.out.println(s);
	}

	public static void print(String s) {
		// System.out.print(s);
	}

	public static Object call(Object object, String methodName, Object... arguments) throws Exception {
		Method method = Proxist.getMethod(object, methodName, arguments);
		return method.invoke(object, arguments);
	}

	public static Object callExec(Object dhasher, Object id, String remoteMethodName, Object... arguments) throws Exception {
		Method method = dhasher.getClass().getMethod("exec", Object.class, String.class, Object[].class);
		return method.invoke(dhasher, id, remoteMethodName, arguments);
	}

	private static ClassLoader getClassLoader(String classPathString) throws Exception {
		String[] classPathParts = classPathString.split(":");
		URL[] classPath = new URL[classPathParts.length];
		for (int i = 0; i < classPath.length; i++) {
			classPath[i] = new URL("file://" + new File(classPathParts[i]).getAbsolutePath());
		}
		return new URLClassLoader(classPath, null);
	}

	private ClassLoader sl;

	private ClassLoader dl;

	private Object sd;

	private Object dd;

	private Map<String, Method> specialCases;

	private long l;

	private HashMap<Object, Object> foundListsById;

	private HashSet<Object> foundLists;

	private HashSet<Object> mapReferencedLists;

	private void notifyUpgrade(Object i, Object o) {
		println("\rUpgrading " + i + " => " + o);
	}

	private void dot() {
		System.out.print(".");
	}

	private void upgradeDCollection(Object identifier, Object object, Object newCollection) throws Exception {
		notifyUpgrade(identifier, object);
		Object destinationIdentifier = toDestinationIdentifier(identifier);
		call(newCollection, "setId", destinationIdentifier);
		call(call(dd, "put", destinationIdentifier, newCollection), "get");
		Object sourceIterator = call(callExec(sd, identifier, "iterator"), "get");
		l++;
		while (Boolean.TRUE.equals(call(sourceIterator, "hasNext", sd))) {
			println("<adding>");
			call(callExec(dd, destinationIdentifier, "add", call(sourceIterator, "next", sd)), "get");
			l++;
		}
	}

	@SuppressWarnings("unchecked")
	private void upgradeDMap(Object identifier, Object object, Object newMap) throws Exception {
		notifyUpgrade(identifier, object);
		Object destinationIdentifier = toDestinationIdentifier(identifier);
		call(newMap, "setId", destinationIdentifier);
		call(call(dd, "put", destinationIdentifier, newMap), "get");
		Object sourceIterator = call(callExec(sd, identifier, "iterator"), "get");
		l++;
		while (Boolean.TRUE.equals(call(sourceIterator, "hasNext", sd))) {
			println("<adding>");
			Map.Entry<Object, Object> next = (Map.Entry<Object, Object>) call(sourceIterator, "next", sd);
			call(callExec(dd, destinationIdentifier, "put", next.getKey(), next.getValue()), "get");
			l++;
		}
	}

	private void copyEntry(Object identifier, Object object) throws Exception {
		if (object == null) {
			println("\rcopying null");
			call(call(dd, "put", toDestinationIdentifier(identifier), null), "get");
		} else {
			println("\rcopying " + call(object, "getClass"));
			call(call(dd, "put", toDestinationIdentifier(identifier), dl.loadClass("cx.ath.troja.nja.Cerealizer").getMethod("unpack", byte[].class)
					.invoke(null, sl.loadClass("cx.ath.troja.nja.Cerealizer").getMethod("pack", Object.class).invoke(null, object))), "get");
		}
		l++;
	}

	/*
	 * Don't copy it, it will be copied when the containing map is copied
	 */
	public void upgradeDHashMap_DHashMapEntry(Object identifier, Object object) {
	}

	/*
	 * Copy the entire map using an iterator, and remember the list it used to point to
	 */
	public void upgradeDHashMap(Object identifier, Object object) throws Exception {
		// "list" here is specific for schema version 0, remember to change if you want to upgrade from other version
		mapReferencedLists.add(call(object, "list"));
		upgradeDMap(identifier, object, instance(dl, "cx.ath.troja.chordless.dhash.structures.DHashMap"));
	}

	/*
	 * Copy the entire set using an iterator, and remember the list it used to point to
	 */
	public void upgradeDSet(Object identifier, Object object) throws Exception {
		// "list" here is specific for schema version 0, remember to change if you want to upgrade from other version
		mapReferencedLists.add(call(callExec(sd, identifier, "list"), "get"));
		upgradeDCollection(identifier, object, instance(dl, "cx.ath.troja.chordless.dhash.structures.DSet"));
	}

	/*
	 * Don't copy it, it will be copied when the containing treap is copied
	 */
	public void upgradeDTreap_Node(Object identifier, Object object) {
	}

	/*
	 * Copy the entire treap using an iterator
	 */
	public void upgradeDTreap(Object identifier, Object object) throws Exception {
		upgradeDMap(identifier, object, instance(dl, "cx.ath.troja.chordless.dhash.structures.DTreap"));
	}

	/*
	 * Don't copy it, it will be copied when the containing tree is copied
	 */
	public void upgradeDTree_Node(Object identifier, Object object) {
	}

	/*
	 * Copy the entire treap using an iterator
	 */
	public void upgradeDTree(Object identifier, Object object) throws Exception {
		upgradeDCollection(identifier, object, instance(dl, "cx.ath.troja.chordless.dhash.structures.DTree"));
	}

	/*
	 * Copy the entire set using an iterator
	 */
	public void upgradeDSortedSet(Object identifier, Object object) throws Exception {
		upgradeDCollection(identifier, object, instance(dl, "cx.ath.troja.chordless.dhash.structures.DSortedSet"));
	}

	/*
	 * Copy the element as is
	 */
	public void upgradeDIndexedList(Object identifier, Object object) throws Exception {
		copyEntry(identifier, object);
	}

	/*
	 * Remember the list
	 */
	public void upgradeDList(Object identifier, Object object) throws Exception {
		foundLists.add(identifier);
		foundListsById.put(identifier, object);
	}

	/*
	 * Do nothing, it will be copied as part of its list if the list is not a part of a DHashMap
	 */
	public void upgradeDList_Element(Object identifier, Object object) throws Exception {
	}

	public Object toDestinationIdentifier(Object sourceIdentifier) throws Exception {
		return instance(dl, "cx.ath.troja.nja.Identifier", sourceIdentifier.toString());
	}

	@SuppressWarnings("unchecked")
	public Upgrade(RuntimeArguments arguments) throws Exception {
		sl = getClassLoader(arguments.mustGet("sourceClassPath", "You must provide " + this + " with a 'sourceClassPath=' argument"));
		dl = getClassLoader(arguments.mustGet("destinationClassPath", "You must provide " + this + " with a 'destinationClassPath=' argument"));

		sd = instance(
				sl,
				"cx.ath.troja.chordless.dhash.RemoteDhasher",
				new InetSocketAddress(arguments.mustGet("sourceAddress", "You must provide " + this + " with a 'sourceAddress=' argument"), Integer
						.parseInt(arguments.mustGet("sourcePort", "You must provide " + this + " with a 'sourcePort=' argument"))));
		dd = instance(dl, "cx.ath.troja.chordless.dhash.RemoteDhasher",
				new InetSocketAddress(arguments.mustGet("destinationAddress", "You must provide " + this + " with a 'destinationAddress=' argument"),
						Integer.parseInt(arguments.mustGet("destinationPort", "You must provide " + this + " with a 'destinationPort=' argument"))));
		Object sourceSchemaVersion = call(
				call(sd, "get", sl.loadClass("cx.ath.troja.chordless.dhash.DHash").getField("SCHEMA_VERSION_KEY").get(null)), "get");
		Object destinationSchemaVersion = call(
				call(dd, "get", dl.loadClass("cx.ath.troja.chordless.dhash.DHash").getField("SCHEMA_VERSION_KEY").get(null)), "get");

		if (!sourceSchemaVersion.toString().equals("0") || !destinationSchemaVersion.toString().equals("1"))
			throw new RuntimeException("" + this + " can only upgrade from schema version 0 to schema version 1");

		println("Going to upgrade " + sd + " (v" + sourceSchemaVersion + ") to " + dd + " (v" + destinationSchemaVersion + ")");

		Object firstEntryId = call(call(call(sd, "nextEntry", instance(sl, "cx.ath.troja.nja.Identifier", new Integer(0))), "get"), "getKey");
		Object nextEntry = call(call(sd, "nextEntry", firstEntryId), "get");
		l = 0;
		foundLists = new HashSet<Object>();
		foundListsById = new HashMap<Object, Object>();
		mapReferencedLists = new HashSet<Object>();
		while (!call(nextEntry, "getKey").equals(firstEntryId)) {
			System.out.print("\r" + l + "   ");
			Object key = call(nextEntry, "getKey");
			Object value = call(nextEntry, "getValue");
			String className = value == null ? "null" : (String) call(call(value, "getClass"), "getName");
			if (className.matches("^cx\\.ath\\.troja\\.chordless\\.dhash\\.structures\\..*")) {
				String[] classNameParts = (value == null ? "null" : (String) call(call(value, "getClass"), "getName")).split("\\.");
				String base = classNameParts[classNameParts.length - 1].replaceAll("\\W+", "_");
				call(this, "upgrade" + base, key, value);
			} else if (className.equals("cx.ath.troja.chordless.dhash.transactions.TransactionBackend")) {
				l++;
			} else {
				copyEntry(key, value);
			}
			nextEntry = call(call(sd, "nextEntry", key), "get");
		}
		foundLists.removeAll(mapReferencedLists);
		for (Object identifier : foundLists) {
			System.out.print("\r" + l);
			Object object = foundListsById.get(identifier);
			upgradeDCollection(identifier, object, instance(dl, "cx.ath.troja.chordless.dhash.structures.DList"));
		}
		call(call(dd, "put", dl.loadClass("cx.ath.troja.chordless.dhash.DHash").getField("SCHEMA_VERSION_KEY").get(null), destinationSchemaVersion),
				"get");
		call(sd, "stop");
		call(dd, "stop");
	}

}
