/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.warn;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Kompiler {

	private static class DummyClassLoader extends ClassLoader {
		public DummyClassLoader(ClassLoader parent) {
			super(parent);
		}

		public Class publicDefine(String className, byte[] classData) {
			return defineClass(className, classData, 0, classData.length);
		}
	}

	private static Kompiler instance = null;

	public static Kompiler getInstance() {
		if (instance == null) {
			instance = new Kompiler();
		}
		return instance;
	}

	private Method compilerMethod;

	private Pattern classNamePattern = Pattern.compile(".*public\\s+(class|interface)\\s+([^\\s<]+)[\\s<].*", Pattern.MULTILINE | Pattern.DOTALL);

	private File getToolsJar() {
		File javaHome = new File(System.getProperty("java.home"));
		File toolsJar = null;
		if (System.getProperty("os.name").equals("Mac OS X")) {
			toolsJar = new File(new File(javaHome.getParentFile(), "Classes"), "classes.jar");
		} else {
			toolsJar = new File(new File(javaHome.getParentFile(), "lib"), "tools.jar");
		}
		if (!toolsJar.exists()) {
			throw new RuntimeException("Unable to find " + toolsJar);
		}
		return toolsJar;
	}

	private Kompiler() {
		try {
			Class<?> compilerClass = new URLClassLoader(new URL[] { getToolsJar().toURI().toURL() }).loadClass("com.sun.tools.javac.Main");
			compilerMethod = compilerClass.getMethod("compile", new Class[] { String[].class, PrintWriter.class });
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private File flush(String code) {
		try {
			Matcher classNameMatcher = classNamePattern.matcher(code);
			if (!classNameMatcher.matches()) {
				throw new RuntimeException("Unable to find a public class definition (" + classNamePattern + ") in '" + code + "'");
			}
			File destination = File.createTempFile(this.getClass().getName(), ".src");
			destination.delete();
			destination.mkdir();
			File returnValue = new File(destination, classNameMatcher.group(2) + ".java");
			PrintStream printer = new PrintStream(returnValue);
			printer.print(code);
			printer.close();
			return returnValue;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void delete(File f) {
		if (f.isDirectory()) {
			File[] children = f.listFiles();
			for (int i = 0; i < children.length; i++) {
				delete(children[i]);
			}
			f.delete();
		} else {
			f.delete();
		}
	}

	private File getFile(File f) {
		if (f.isDirectory()) {
			File[] children = f.listFiles();
			if (children.length != 1) {
				throw new RuntimeException("" + f + " contained more than one file!");
			}
			return getFile(children[0]);
		} else {
			return f;
		}
	}

	private File compile(File source) {
		try {
			File destination = File.createTempFile(this.getClass().getName(), ".classes");
			destination.delete();
			destination.mkdir();
			StringWriter errors = new StringWriter();
			compilerMethod.invoke(null,
					new String[] { "-classpath", System.getProperty("java.class.path"), "-Xlint:unchecked", "-d", destination.getCanonicalPath(),
							source.getCanonicalPath() }, new PrintWriter(errors));
			if (errors.toString().length() > 0) {
				throw new RuntimeException("Error compiling '" + source + "': '" + errors.toString() + "'");
			}
			return destination;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private byte[] getBytes(File f) {
		try {
			FileInputStream input = new FileInputStream(f);
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			byte[] bytes = new byte[1024];
			int n;
			while ((n = input.read(bytes)) != -1) {
				byteOut.write(bytes, 0, n);
			}
			return byteOut.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Class loadBytes(File root, File bytecodeFile) {
		try {
			byte[] bytes = getBytes(bytecodeFile);
			String className = bytecodeFile.getCanonicalPath().substring(root.getCanonicalPath().length() + 1)
					.replace(System.getProperty("file.separator"), ".").replace(".class", "");
			DummyClassLoader loader = new DummyClassLoader(this.getClass().getClassLoader());
			Class returnValue = loader.publicDefine(className, bytes);
			returnValue.forName(returnValue.getName(), true, loader);
			return returnValue;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private String fileCollectionToPath(Collection<File> c) {
		try {
			StringBuffer returnValue = new StringBuffer();
			Iterator<File> iterator = c.iterator();
			while (iterator.hasNext()) {
				returnValue.append(iterator.next().getCanonicalPath());
				if (iterator.hasNext()) {
					returnValue.append(System.getProperty("path.separator"));
				}
			}
			return returnValue.toString();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void compile(Collection<File> source, Collection<File> classpath, Collection<File> sourcepath) {
		try {
			String[] arguments = new String[5 + source.size()];
			arguments[0] = "-classpath";
			arguments[1] = fileCollectionToPath(classpath);
			arguments[2] = "-sourcepath";
			arguments[3] = fileCollectionToPath(sourcepath);
			arguments[4] = "-Xlint:unchecked";
			int i = 5;
			for (File s : source) {
				arguments[i] = s.getCanonicalPath();
				i++;
			}
			StringWriter errors = new StringWriter();
			compilerMethod.invoke(null, arguments, new PrintWriter(errors));
			if (errors.toString().length() > 0) {
				warn(this, errors.toString());
				throw new RuntimeException("Error compiling '" + source + "' with classpath '" + classpath + "' and sourcepath '" + sourcepath + "'");
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> Class<T> compile(String code) {
		try {
			File sourceFile = flush(code);
			File destination = compile(sourceFile);
			Class returnValue = loadBytes(destination, getFile(destination));
			delete(sourceFile.getParentFile());
			delete(destination);
			return (Class<T>) returnValue;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
