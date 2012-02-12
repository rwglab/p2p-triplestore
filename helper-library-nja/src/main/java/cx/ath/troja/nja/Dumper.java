/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Creates or updates an instances of a class to a database.
 * 
 * Use JDBC#getDumper to get an instance.
 */
public class Dumper {

	public static class IllegalDumpException extends RuntimeException {
		public IllegalDumpException(String s) {
			super(s);
		}
	}

	public interface IdGetter {
		public Long get(JDBC j);
	}

	public static Timestamp nowUTC() {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MILLISECOND, -(calendar.get(Calendar.DST_OFFSET) + calendar.get(Calendar.ZONE_OFFSET)));
		return new Timestamp(calendar.getTimeInMillis());
	}

	private String tableName;

	private Collection<String> fields;

	private IdGetter idGetter;

	private Class<? extends Object> klass;

	private String idField;

	private JDBC.JDBCPool jdbcPool;

	protected Dumper(JDBC.JDBCPool j, Class<? extends Object> c, String tn, Collection<String> f, String idf, IdGetter igc) {
		jdbcPool = j;
		klass = c;
		tableName = tn;
		fields = f;
		idField = idf;
		idGetter = igc;
	}

	public JDBC getJDBC() {
		return jdbcPool.acquire();
	}

	public String toString() {
		return "<" + this.getClass().getName() + " jdbcPool=" + jdbcPool + " klass=" + klass + " tableName=" + tableName + " fields=" + fields
				+ " idField=" + idField + " idGetter=" + idGetter + ">";
	}

	private Object get(Object object, String field) throws Exception {
		try {
			return Proxist.getMethod(object, "get" + JDBC.capitalize(JDBC.camelize(field))).invoke(object);
		} catch (NoSuchMethodException e) {
			Field f = Dumper.this.klass.getField(JDBC.camelize(field));
			return f.get(object);
		}
	}

	private void appendSelector(Object o, StringBuffer query, List<Object> parameters) {
		try {
			if (idField == null) {
				Iterator<String> iterator = fields.iterator();
				while (iterator.hasNext()) {
					String field = iterator.next();
					query.append(field).append(" = ?");
					parameters.add(get(o, field));
					if (iterator.hasNext()) {
						query.append(" AND ");
					}
				}
			} else {
				query.append(idField).append(" = ?");
				parameters.add(get(o, idField));
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Remove the object from the database.
	 * 
	 * @param o
	 *            the object to remove
	 */
	public void delete(Object o) {
		try {
			if (!klass.isAssignableFrom(o.getClass())) {
				throw new IllegalDumpException("" + this + " only deletes instances of " + klass);
			}
			StringBuffer query = new StringBuffer("DELETE FROM ").append(tableName).append(" WHERE ");
			List<Object> parameters = new ArrayList<Object>();
			appendSelector(o, query, parameters);
			int updated = jdbcPool.acquire().execute(query.toString(), parameters.toArray(new Object[0]));
			if (updated != 1) {
				throw new RuntimeException("Failed to delete " + o + " using " + query + " and " + parameters + ", query updated " + updated
						+ " rows isntead of exactly one");
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * If the object has a known id attribute it will be updated, otherwise created, in the database.
	 * 
	 * @param o
	 *            the object to update or create
	 */
	public void dump(Object o) {
		if (!klass.isAssignableFrom(o.getClass())) {
			throw new IllegalDumpException("" + this + " only updates instances of " + klass);
		}

		try {
			if (idField != null && get(o, idField) != null) {
				update(o);
			} else {
				create(o);
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Updates the object in the database.
	 * 
	 * Will fail if the object does not have an id.
	 * 
	 * @param o
	 *            the object to update
	 */
	public void update(Object o) {
		if (!klass.isAssignableFrom(o.getClass())) {
			throw new IllegalDumpException("" + this + " only updates instances of " + klass);
		}

		try {
			if (idField != null && get(o, idField) == null) {
				throw new IllegalDumpException("Failed updating " + o + ", it doesn't have an id");
			}

			StringBuffer query = new StringBuffer();
			List<Object> parameters = new ArrayList<Object>();
			query.append("UPDATE ").append(tableName).append(" SET ");
			Iterator<String> iterator = fields.iterator();
			Timestamp now = nowUTC();
			while (iterator.hasNext()) {
				String field = iterator.next();
				query.append(field).append(" = ?");
				if (field.equals("updated_at")) {
					parameters.add(now);
					Loader.set(o, field, now);
				} else {
					parameters.add(get(o, field));
				}
				if (iterator.hasNext()) {
					query.append(", ");
				}
			}
			query.append(" WHERE ");
			appendSelector(o, query, parameters);
			int updated = jdbcPool.acquire().execute(query.toString(), parameters.toArray(new Object[0]));
			if (updated != 1) {
				throw new RuntimeException("Failed to update " + o + " using " + query + " and " + parameters + ", query updated " + updated
						+ " rows isntead of exactly one");
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create the object in the database.
	 * 
	 * Will fail if the object already has an id.
	 * 
	 * @param o
	 *            the object to create
	 */
	public void create(Object o) {
		if (!klass.isAssignableFrom(o.getClass())) {
			throw new IllegalDumpException("" + this + " only dumps instances of " + klass);
		}

		try {
			if (idField != null && get(o, idField) != null) {
				throw new IllegalDumpException("Failed to create " + o + ", it already has an id");
			}

			StringBuffer query = new StringBuffer();
			List<Object> parameters = new ArrayList<Object>();
			query.append("INSERT INTO ").append(tableName).append(" (");
			Iterator<String> iterator = fields.iterator();
			Timestamp now = nowUTC();
			while (iterator.hasNext()) {
				String field = iterator.next();
				query.append(field);
				if (field.equals("updated_at") || field.equals("created_at")) {
					parameters.add(now);
					Loader.set(o, field, now);
				} else {
					parameters.add(get(o, field));
				}
				if (iterator.hasNext()) {
					query.append(", ");
				}
			}
			query.append(") VALUES (");
			iterator = fields.iterator();
			while (iterator.hasNext()) {
				String field = iterator.next();
				query.append("?");
				if (iterator.hasNext()) {
					query.append(", ");
				}
			}
			query.append(")");
			int updated = jdbcPool.acquire().execute(query.toString(), parameters.toArray(new Object[0]));
			if (updated != 1) {
				throw new RuntimeException("Failed to save " + o + " using " + query + " and " + parameters + ", query updated " + updated
						+ " rows instead of exactly one");
			}
			if (idField != null && idGetter != null) {
				Loader.set(o, idField, idGetter.get(jdbcPool.acquire()));
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
