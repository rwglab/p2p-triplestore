/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import static cx.ath.troja.nja.Log.debug;
import static cx.ath.troja.nja.Log.warn;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Loads instances of a class from a database.
 * 
 * Use JDBC#getLoader to get an instance.
 */
public class Loader<T> {

	public static void set(Object object, String field, Object value) throws Exception {
		try {
			Proxist.getMethod(object, "set" + JDBC.capitalize(JDBC.camelize(field)), value).invoke(object, value);
		} catch (NoSuchMethodException e) {
			Field f = object.getClass().getField(JDBC.camelize(field));
			f.set(object, value);
		}
	}

	private interface ParameterAppender {
		public void append(Object value);
	}

	/**
	 * Matches criteria to fetch one or more instances from the database.
	 * 
	 * Use Loader.m() to get a Matcher.
	 */
	public class Matcher {
		private String order;

		private Integer limit;

		private Integer offset;

		private StringBuffer conditions;

		private List<Object> parameters;

		private String lockType;

		public Matcher() {
			lockType = null;
			order = null;
			limit = null;
			offset = null;
			conditions = new StringBuffer();
			parameters = new ArrayList<Object>();
		}

		private Matcher appendCriteria(ParameterAppender appender, Object... arguments) {
			if (arguments.length > 0) {
				if (conditions.length() > 0) {
					conditions.append(" AND ");
				}
				conditions.append("(");
				for (int i = 0; i < arguments.length; i = i + 2) {
					conditions.append("").append(arguments[i]);
					appender.append(arguments[i + 1]);
					if (i + 2 < arguments.length) {
						conditions.append(" AND ");
					}
				}
				conditions.append(")");
			}
			return this;
		}

		/**
		 * Adds criteria to the Matcher.
		 * 
		 * @param arguments
		 *            the fields that should match the loaded instances. The first argument is the column name, the
		 *            second the object that it should be greater than, the third and fourth arguments define the next
		 *            column to match against etc.
		 * @return this Matcher
		 */
		public Matcher greater(Object... arguments) {
			return appendCriteria(new ParameterAppender() {
				public void append(Object value) {
					if (value == null) {
						throw new RuntimeException("Unable to compare null");
					} else {
						conditions.append(" > ?");
						parameters.add(value);
					}
				}
			}, arguments);
		}

		/**
		 * Adds criteria to the Matcher.
		 * 
		 * @param arguments
		 *            the fields that should match the loaded instances. The first argument is the column name, the
		 *            second the object that it should be lesser than, the third and fourth arguments define the next
		 *            column to match against etc.
		 * @return this Matcher
		 */
		public Matcher lesser(Object... arguments) {
			return appendCriteria(new ParameterAppender() {
				public void append(Object value) {
					if (value == null) {
						throw new RuntimeException("Unable to compare null");
					} else {
						conditions.append(" < ?");
						parameters.add(value);
					}
				}
			}, arguments);
		}

		/**
		 * Adds criteria to the Matcher.
		 * 
		 * @param arguments
		 *            the fields that should match the loaded instances. The first argument is the column name, the
		 *            second the object that it should NOT equal, the third and fourth arguments define the next column
		 *            to match against etc.
		 * @return this Matcher
		 */
		public Matcher unmatch(Object... arguments) {
			return appendCriteria(new ParameterAppender() {
				public void append(Object value) {
					if (value == null) {
						conditions.append(" IS NOT NULL");
					} else {
						conditions.append(" != ?");
						parameters.add(value);
					}
				}
			}, arguments);
		}

		/**
		 * Adds criteria to the Matcher.
		 * 
		 * @param arguments
		 *            the fields that should match the loaded instances. The first argument is the column name, the
		 *            second the object that it should equal, the third and fourth arguments define the next column to
		 *            match against etc.
		 * @return this Matcher
		 */
		public Matcher match(Object... arguments) {
			return appendCriteria(new ParameterAppender() {
				public void append(Object value) {
					if (value == null) {
						conditions.append(" IS NULL");
					} else {
						conditions.append(" = ?");
						parameters.add(value);
					}
				}
			}, arguments);
		}

		/**
		 * Adds criteria to the matcher
		 * 
		 * @param condition
		 *            a string that will be appended to the query
		 * @param params
		 *            the parameters that will be appended to the query
		 * @return this Matcher
		 */
		public Matcher where(String condition, Object... params) {
			if (conditions.length() > 0) {
				conditions.append(" AND ");
			}
			conditions.append("(").append(condition).append(")");
			for (int i = 0; i < params.length; i++) {
				parameters.add(params[i]);
			}
			return this;
		}

		/**
		 * Makes this match lock the resulting rows.
		 * 
		 * With PostgreSQL, the legal types are "UPDATE" and "SHARE", where "UPDATE" blocks modifications and other
		 * "UPDATE" locks, and "SHARE" only blocks modifications.
		 * 
		 * @param type
		 *            The lock type to acquire, right now "UPDATE" or "SHARE"
		 */
		public Matcher lock(String type) {
			if (type.equals("UPDATE")) {
				lockType = type;
			} else if (type.equals("SHARE")) {
				lockType = type;
			} else {
				throw new RuntimeException("Unknown lock type " + type);
			}
			return this;
		}

		public String toString() {
			return "<Matcher " + generateQuery() + ", " + parameters + ">";
		}

		private String generateQuery(String selection) {
			StringBuffer finalQuery = new StringBuffer("SELECT " + selection + " FROM ").append(Loader.this.tableName);
			if (conditions.length() > 0) {
				finalQuery.append(" WHERE ").append(conditions);
			}
			if (order != null) {
				finalQuery.append(" ORDER BY ").append(order);
			}
			if (limit != null) {
				finalQuery.append(" LIMIT ").append(limit);
			}
			if (offset != null) {
				finalQuery.append(" OFFSET ").append(offset);
			}
			if (lockType != null) {
				finalQuery.append(" FOR ").append(lockType);
			}
			return finalQuery.toString();
		}

		private String generateQuery() {
			return generateQuery("*");
		}

		private ResultSetWrapper getResult() {
			String finalQuery = generateQuery();
			debug(this, "Going to run " + finalQuery + " with " + parameters);
			try {
				return Loader.this.jdbcPool.acquire().query(finalQuery, parameters.toArray(new Object[0]));
			} catch (RuntimeException e) {
				warn(this, "Error running " + finalQuery + " with " + parameters);
				throw e;
			}
		}

		/**
		 * @return a List of all the matching instances
		 */
		public List<T> all() {
			List<T> returnValue = new ArrayList<T>();
			ResultSetWrapper wrapper = getResult();
			while (wrapper.next()) {
				returnValue.add(resultSetToObject(wrapper));
			}
			return returnValue;
		}

		/**
		 * @return the number of matches
		 */
		public Long count() {
			String finalQuery = generateQuery("COUNT(*) AS count");
			debug(this, "Going to run " + finalQuery + " with " + parameters);
			try {
				return Loader.this.jdbcPool.acquire().query(Long.class, "count", finalQuery, parameters.toArray(new Object[0]));
			} catch (RuntimeException e) {
				warn(this, "Error running " + finalQuery + " with " + parameters);
				throw e;
			}
		}

		/**
		 * @return the first matching instance
		 */
		public T first() {
			Integer oldLimit = limit;
			limit = new Integer(1);
			try {
				ResultSetWrapper wrapper = getResult();
				if (wrapper.next()) {
					T returnValue = resultSetToObject(wrapper);
					wrapper.close();
					return returnValue;
				} else {
					return null;
				}
			} finally {
				limit = oldLimit;
			}
		}

		/**
		 * Change the order of the query
		 * 
		 * @param order
		 *            the columns to order by
		 * @return this Matcher
		 */
		public Matcher order(String o) {
			order = o;
			return this;
		}

		/**
		 * Change the limit of the query
		 * 
		 * @param i
		 *            the max number of instances to return
		 * @return this Matcher
		 */
		public Matcher limit(int i) {
			limit = new Integer(i);
			return this;
		}

		/**
		 * Change the offset of the query
		 * 
		 * @param i
		 *            the row number to begin fetching from
		 * @return this Matcher
		 */
		public Matcher offset(int i) {
			offset = new Integer(i);
			return this;
		}
	}

	private String tableName;

	private Collection<String> fields;

	private JDBC.JDBCPool jdbcPool;

	private Class<T> klass;

	private String idField;

	protected Loader(JDBC.JDBCPool j, Class<T> c, String tn, Collection<String> f, String idf) {
		jdbcPool = j;
		klass = c;
		tableName = tn;
		fields = f;
		idField = idf;
	}

	public String toString() {
		return "<" + this.getClass().getName() + " jdbcPool=" + jdbcPool + " klass=" + klass + "tableName=" + tableName + " fields=" + fields
				+ " idField=" + idField + ">";
	}

	public Matcher m() {
		return new Matcher();
	}

	/**
	 * Returns a new instance of the class this Loader loads created from the content in the current row of wrapper.
	 * 
	 * @param wrapper
	 *            The ResultSetWrapper to fetch attributes from
	 */
	public T resultSetToObject(ResultSetWrapper wrapper) {
		return resultSetToObject(wrapper, "");
	}

	/**
	 * Returns a new instance of the class this Loader loads created from the content in the current row of wrapper.
	 * 
	 * @param wrapper
	 *            The ResultSetWrapper to fetch attributes from
	 * @param prefix
	 *            The prefix to append to each field in wrapper
	 */
	public T resultSetToObject(ResultSetWrapper wrapper, String prefix) {
		try {
			T returnValue = klass.newInstance();
			for (String field : fields) {
				Object value = wrapper.get(prefix + field);
				if (field.equals(idField)) {
					value = new Long(((Integer) value).longValue());
				}
				set(returnValue, field, value);
			}
			return returnValue;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Loads the instance with the given id.
	 * 
	 * @param id
	 *            the id of the instance to load
	 * @return an instance of the configured class from the database, having the given id
	 */
	public T load(Integer id) {
		if (idField == null) {
			throw new RuntimeException("" + this + " is unable to load from id, since it has no knowledge of what field is the id field");
		}
		return m().match(idField, id).first();
	}
}
