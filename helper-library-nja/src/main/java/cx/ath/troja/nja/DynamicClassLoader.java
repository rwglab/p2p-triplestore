/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.DEBUG;
import static cx.ath.troja.nja.Log.TRACE;
import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.loggable;
import static cx.ath.troja.nja.Log.trace;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A class that makes it easy to load classes over a serial pipe.
 */
public class DynamicClassLoader extends ClassLoader {

	private class DummyClassLoader extends ClassLoader {
		public DummyClassLoader(ClassLoader parent) {
			super(parent);
		}

		public Class publicDefine(String className, byte[] classData) {
			return defineClass(className, classData, 0, classData.length);
		}

		@Override
		public Class findClass(String className) throws ClassNotFoundException {
			Class returnValue = DynamicClassLoader.this.getClassByName().get(className);
			if (returnValue == null) {
				throw new ClassNotFoundException(className);
			} else {
				return returnValue;
			}
		}
	}

	private class Dependent {
		String name;

		byte[] data;

		public Dependent(String n, byte[] d) {
			name = n;
			data = d;
		}

		public String getName() {
			return name;
		}

		public String toString() {
			return "<Dependent " + name + ">";
		}

		public void add() {
			if (loggable(this, DEBUG)) {
				debug(this, "Adding " + this + " because");
			}
			DynamicClassLoader.this.addClass(name, data);
		}
	}

	private Map<String, Class> classByName = new HashMap<String, Class>();

	private Map<String, String> nameByDependency = new HashMap<String, String>();

	private Map<String, Set<Dependent>> dependenciesByName = new HashMap<String, Set<Dependent>>();

	private Set<String> properlyLoaded = new HashSet<String>();

	protected Map<String, Class> getClassByName() {
		return classByName;
	}

	public boolean hasClass(String name) {
		try {
			new DummyClassLoader(this).loadClass(name);
			return true;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	/**
	 * Create a loader with the system class loader as parent.
	 */
	public DynamicClassLoader() {
		this(ClassLoader.getSystemClassLoader());
	}

	/**
	 * {@inheritDoc}
	 */
	public DynamicClassLoader(ClassLoader parent) {
		super(parent);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class findClass(String className) throws ClassNotFoundException {
		if (loggable(this, TRACE)) {
			trace(this, "Asked to find " + className);
		}
		String dependsOn = nameByDependency.get(className);
		if (dependsOn != null) {
			throw new ClassNotFoundException(dependsOn);
		}
		Class returnValue = classByName.get(className);
		if (returnValue == null) {
			if (loggable(this, DEBUG)) {
				debug(this, "Couldn't find " + className + ", throwing exception!");
			}
			throw new ClassNotFoundException(className);
		} else {
			return returnValue;
		}
	}

	private void addDependent(String className, Dependent dep) {
		Set<Dependent> dependencies = dependenciesByName.get(className);
		if (dependencies == null) {
			dependencies = new HashSet<Dependent>();
			dependenciesByName.put(className, dependencies);
		}
		dependencies.add(dep);
		nameByDependency.put(dep.getName(), className);
	}

	private void resolveDependencies(String className) throws Exception {
		properlyLoaded.add(className);
		Set<Dependent> dependencies = dependenciesByName.get(className);
		if (dependencies != null) {
			for (Dependent dep : dependencies) {
				nameByDependency.remove(dep.getName());
				dep.add();
			}
		}
	}

	/**
	 * Add a class to this loader.
	 * 
	 * @param className
	 *            the name of the class
	 * @param classData
	 *            the bytes that consist this class
	 */
	public synchronized void addClass(String className, byte[] classData) {
		if (loggable(this, DEBUG)) {
			debug(this, "Asked to add " + className);
		}
		if (!properlyLoaded.contains(className)) {
			try {
				DummyClassLoader dummy = new DummyClassLoader(this);
				Class tmpClass = dummy.publicDefine(className, classData);
				classByName.put(className, tmpClass);
				Class<?> streamClass = dummy.loadClass("java.io.ObjectStreamClass");
				streamClass.getDeclaredMethod("lookup", Class.class).invoke(streamClass, tmpClass);
				tmpClass = defineClass(className, classData, 0, classData.length);
				classByName.put(className, tmpClass);
				resolveDependencies(className);
			} catch (InvocationTargetException e) {
				if (e.getCause() instanceof NoClassDefFoundError) {
					if (loggable(this, DEBUG)) {
						debug(this, "Unable to add " + className + ", " + e.getCause().getCause().getMessage() + " is missing");
					}
					addDependent(e.getCause().getCause().getMessage(), new Dependent(className, classData));
					throw new Cerealizer.ClassNotFoundException(e.getCause(), e.getCause().getCause().getMessage());
				} else {
					throw new RuntimeException(e);
				}
			} catch (Cerealizer.ClassNotFoundException e) {
				throw e;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

}