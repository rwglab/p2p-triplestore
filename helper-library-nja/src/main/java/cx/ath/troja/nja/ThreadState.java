/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2011 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.util.HashMap;
import java.util.Map;

public class ThreadState {

	private static final Map<Thread, Map<Object, Object>> mapByThread = new ReferenceMap<Thread, Map<Object, Object>>(
			new HashMap<ReferenceMap.Referrable<Thread, Map<Object, Object>>, ReferenceMap.Referrable<Map<Object, Object>, ReferenceMap.Referrable<Thread, Map<Object, Object>>>>(),
			ReferenceMap.WeakRef.class, ReferenceMap.HardRef.class);

	public static Map<Thread, Map<Object, Object>> _getMap() {
		return mapByThread;
	}

	private synchronized static Map<Object, Object> getMap(Thread thread) {
		if (!mapByThread.containsKey(thread)) {
			mapByThread.put(thread, new HashMap<Object, Object>());
		}
		return mapByThread.get(thread);
	}

	public static void put(Thread thread, Object key, Object value) {
		getMap(thread).put(key, value);
	}

	public static void remove(Thread thread, Object key) {
		getMap(thread).remove(key);
	}

	public static Object get(Thread thread, Object key) {
		return getMap(thread).get(key);
	}

	public static void put(Object key, Object value) {
		put(Thread.currentThread(), key, value);
	}

	public static void remove(Object key) {
		remove(Thread.currentThread(), key);
	}

	public static Object get(Object key) {
		return get(Thread.currentThread(), key);
	}

}