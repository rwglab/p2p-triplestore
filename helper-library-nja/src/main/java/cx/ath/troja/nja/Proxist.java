/*
 * This file is part of Nja. Chordless is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Chordless is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public
 * License for more details. You should have received a copy of the GNU General Public License along with Chordless. If
 * not, see <http://www.gnu.org/licenses/>. Copyright 2010 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Proxist {

	public static boolean samePrimitive(Class possiblePrimitive, Class klass) {
		return (possiblePrimitive == byte.class && klass == Byte.class || possiblePrimitive == short.class && klass == Short.class
				|| possiblePrimitive == int.class && klass == Integer.class || possiblePrimitive == long.class && klass == Long.class
				|| possiblePrimitive == double.class && klass == Double.class || possiblePrimitive == boolean.class && klass == Boolean.class || possiblePrimitive == char.class
				&& klass == Character.class);
	}

	public static Method matchingMethod(Method[] methods, boolean lax, String methodName, Object... arguments) {
		Class[] parameterTypes;
		for (int i = 0; i < methods.length; i++) {
			if (methods[i].getName().equals(methodName) && (parameterTypes = methods[i].getParameterTypes()).length == arguments.length) {

				boolean isOk = true;
				for (int j = 0; j < parameterTypes.length; j++) {
					if (lax && arguments[j] != null) {
						isOk &= (samePrimitive(parameterTypes[j], arguments[j].getClass()) || parameterTypes[j].isInstance(arguments[j]));
					} else {
						isOk &= ((!parameterTypes[j].isPrimitive() && arguments[j] == null) || parameterTypes[j].isInstance(arguments[j]));
					}
				}
				if (isOk) {
					return methods[i];
				}
			}
		}
		return null;
	}

	public static Method getMethod(Object object, String methodName, Object... arguments) throws NoSuchMethodException {
		Method[] methods = object.getClass().getMethods();
		Method returnValue = matchingMethod(methods, false, methodName, arguments);
		if (returnValue == null) {
			returnValue = matchingMethod(methods, true, methodName, arguments);
		}
		if (returnValue == null) {
			StringBuffer argbuffer = new StringBuffer();
			for (int i = 0; i < arguments.length; i++) {
				argbuffer.append((arguments[i] == null ? "null" : arguments[i].getClass().getName()) + " " + arguments[i]);
				if (i < arguments.length - 1) {
					argbuffer.append(", ");
				}
			}
			throw new NoSuchMethodException("" + object.getClass().getName() + "." + methodName + "(" + argbuffer + ") ");
		} else {
			return returnValue;
		}
	}

	public interface Forwarder {
		public Object forward(String methodName, Object... arguments);
	}

	public static class SimpleForwarder implements Forwarder {
		private Object backend;

		public SimpleForwarder(Object o) {
			backend = o;
		}

		public Object forward(String methodName, Object... arguments) {
			try {
				Method method = Proxist.getMethod(backend, methodName, arguments);
				return method.invoke(backend, arguments);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		public Object getBackend() {
			return backend;
		}

		public boolean equals(Object other) {
			if (other instanceof SimpleForwarder) {
				return ((SimpleForwarder) other).getBackend().equals(getBackend());
			} else {
				return false;
			}
		}
	}

	public interface Proxy {
		public void setForwarder(Forwarder h);

		public Forwarder getForwarder();
	}

	private static Proxist instance = new Proxist();

	public static Proxist getInstance() {
		return instance;
	}

	private Map<Class, Class<Proxy>> proxyByRemoteClass = Collections.synchronizedMap(new HashMap<Class, Class<Proxy>>());

	private Pattern bug6376382workaround = Pattern.compile("(.*)(([^\\s.]+\\.)+[^\\s.]+)\\.\\2(.*)");

	private Pattern parameterNamePattern = Pattern.compile("(.*[(,])(\\S+)([),].*)");

	private boolean legal(Method method) {
		if ((method.getModifiers() & Modifier.STATIC) == 1) {
			return false;
		}
		if (method.getDeclaringClass() == Object.class) {
			return false;
		}
		Type[] params = method.getParameterTypes();
		if (method.getName().equals("equals") && params.length == 1 && params[0] == Object.class) {
			return false;
		}
		if (method.getName().equals("clone") && params.length == 0) {
			return false;
		}
		if (method.getName().equals("toString") && params.length == 0) {
			return false;
		}
		return true;
	}

	private String conversionForPrimitive(Class primitive) {
		if (primitive.getName().equals("int")) {
			return ".intValue()";
		} else if (primitive.getName().equals("long")) {
			return ".longValue()";
		} else if (primitive.getName().equals("byte")) {
			return ".byteValue()";
		} else if (primitive.getName().equals("short")) {
			return ".shortValue()";
		} else if (primitive.getName().equals("double")) {
			return ".doubleValue()";
		} else if (primitive.getName().equals("float")) {
			return ".floatValue()";
		} else if (primitive.getName().equals("boolean")) {
			return ".booleanValue()";
		} else if (primitive.getName().equals("char")) {
			return ".charValue()";
		} else {
			throw new RuntimeException("Unknown primitive type " + primitive);
		}
	}

	private String typeForPrimitive(Class primitive) {
		if (primitive.getName().equals("int")) {
			return "Integer";
		} else if (primitive.getName().equals("long")) {
			return "Long";
		} else if (primitive.getName().equals("byte")) {
			return "Byte";
		} else if (primitive.getName().equals("short")) {
			return "Short";
		} else if (primitive.getName().equals("double")) {
			return "Double";
		} else if (primitive.getName().equals("float")) {
			return "Float";
		} else if (primitive.getName().equals("boolean")) {
			return "Boolean";
		} else if (primitive.getName().equals("char")) {
			return "Char";
		} else {
			throw new RuntimeException("Unknown primitive type " + primitive);
		}
	}

	private void appendMethod(StringBuffer buffer, Method method, Pattern methodNamePattern) {
		int modifiers = method.getModifiers();
		if (legal(method)) {
			buffer.append("  @SuppressWarnings(\"unchecked\")\n");
			buffer.append("  public ");
			String returnTypeName = "";
			if (method.getGenericReturnType() instanceof Class) {
				returnTypeName = ((Class) method.getGenericReturnType()).getName();
			} else {
				returnTypeName = bug6376382workaround.matcher(method.getGenericReturnType().toString()).replaceAll("$1$2$4").replaceAll("\\$", ".");
			}
			buffer.append(returnTypeName);
			buffer.append(" ");
			buffer.append(method.getName());
			buffer.append("(");
			Type[] params = method.getGenericParameterTypes();
			for (int i = 0; i < params.length; i++) {
				if (params[i] instanceof Class) {
					buffer.append(((Class) params[i]).getName());
				} else {
					buffer.append(bug6376382workaround.matcher(params[i].toString()).replaceAll("$1$2$4").replaceAll("\\$", "."));
				}
				buffer.append(" param" + i);
				if (i < params.length - 1) {
					buffer.append(", ");
				}
			}
			buffer.append(") {\n");
			if (!returnTypeName.equals("void")) {
				if (method.getReturnType().isPrimitive()) {
					buffer.append("    return ((" + typeForPrimitive(method.getReturnType()) + ") ");
					appendMethodCall(buffer, method);
					buffer.append(")" + conversionForPrimitive(method.getReturnType()) + ";\n");
				} else {
					buffer.append("    return (" + returnTypeName + ") ");
					appendMethodCall(buffer, method);
					buffer.append(";\n");
				}
			} else {
				buffer.append("    ");
				appendMethodCall(buffer, method);
				buffer.append(";\n");
			}
			buffer.append("  }\n");
		}
	}

	private void appendMethodCall(StringBuffer buffer, Method method) {
		buffer.append("getForwarder().forward(\"" + method.getName() + "\"");
		for (int i = 0; i < method.getParameterTypes().length; i++) {
			buffer.append(", param" + i);
		}
		buffer.append(")");
	}

	public String generateCodeForProxy(Class remoteClass) {
		int lastDot = remoteClass.getName().lastIndexOf(".");
		String className = remoteClass.getName().substring(lastDot + 1);
		String proxyClassName = className + "Proxy";
		String packageName = remoteClass.getName().substring(0, lastDot);
		StringBuffer buffer = new StringBuffer();
		buffer.append("package cx.ath.troja.nja.proxist." + packageName + ";\n");
		buffer.append("import cx.ath.troja.nja.*;\n");
		buffer.append("import java.lang.reflect.*;\n");
		buffer.append("import " + packageName + ".*;\n");
		buffer.append("public class " + proxyClassName);
		StringBuffer typeBuffer = new StringBuffer();
		TypeVariable[] types = remoteClass.getTypeParameters();
		if (types.length > 0) {
			typeBuffer.append("<");
		}
		for (int i = 0; i < types.length; i++) {
			typeBuffer.append(types[i].toString());
			if (i < types.length - 1) {
				typeBuffer.append(",");
			}
		}
		if (types.length > 0) {
			typeBuffer.append(">");
		}
		buffer.append(typeBuffer);
		buffer.append(" extends " + className + typeBuffer + " implements Proxist.Proxy {\n");
		appendProxyMethods(buffer, proxyClassName);
		Pattern methodNamePattern = Pattern.compile("(.*)" + Pattern.quote(remoteClass.getName()) + "\\.([^(\\s]+\\(.*)");
		Method[] methods = remoteClass.getMethods();
		for (int i = 0; i < methods.length; i++) {
			appendMethod(buffer, methods[i], methodNamePattern);
		}
		buffer.append("}\n");
		return buffer.toString();
	}

	private void appendProxyMethods(StringBuffer buffer, String proxyClassName) {
		buffer.append("  private Proxist.Forwarder forwarder;\n");
		buffer.append("  public void setForwarder(Proxist.Forwarder f) {\n");
		buffer.append("    forwarder = f;\n");
		buffer.append("  }\n");
		buffer.append("  public Proxist.Forwarder getForwarder() {\n");
		buffer.append("    return forwarder;\n");
		buffer.append("  }\n");
		buffer.append("  public " + proxyClassName + "() {\n");
		buffer.append("  }\n");
		buffer.append("  public String toString() {\n");
		buffer.append("    return \"<" + proxyClassName + " forwarder='\" + getForwarder() + \"'>\";\n");
		buffer.append("  }\n");
		buffer.append("  public boolean equals(Object other) {\n");
		buffer.append("    if (other instanceof Proxist.Proxy) {\n");
		buffer.append("      return ((Proxist.Proxy) other).getForwarder().equals(getForwarder());\n");
		buffer.append("    } else {\n");
		buffer.append("      return false;\n");
		buffer.append("    }\n");
		buffer.append("  }\n");
	}

	private Class generateProxy(Class remoteClass) {
		return Kompiler.getInstance().compile(generateCodeForProxy(remoteClass));
	}

	@SuppressWarnings("unchecked")
	public Class<Proxy> proxyFor(Class remoteClass) {
		if (!proxyByRemoteClass.containsKey(remoteClass)) {
			proxyByRemoteClass.put(remoteClass, generateProxy(remoteClass));
		}
		return proxyByRemoteClass.get(remoteClass);
	}

}