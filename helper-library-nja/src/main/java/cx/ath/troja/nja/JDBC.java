/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2011 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * A simple class to wrap the horrible horrible way java.sql works.
 */
public class JDBC {

	public interface JDBCCallable<T> extends Callable<T> {
		public void setJDBC(JDBC jdbc);
	}

	public interface TransactionStarter {
		public void start(JDBC jdbc);
	}

	public static class Serial {
	}

	public static String capitalize(String s) {
		return s.substring(0, 1).toUpperCase() + s.substring(1);
	}

	public static String camelize(String s) {
		String[] parts = s.split("_");
		StringBuffer returnValue = new StringBuffer();
		for (int i = 0; i < parts.length; i++) {
			if (i == 0) {
				returnValue.append(parts[i]);
			} else {
				returnValue.append(capitalize(parts[i]));
			}
		}
		return returnValue.toString();
	}

	public static String getTableName(Class k) {
		return k.getName().replaceFirst("^(.*)[.\\$]([^.$]+)$", "$2").toLowerCase();
	}

	private Connection connection;

	private Map<Class, String> classMap;

	private String createIndexFormat = "CREATE {0}INDEX {1} ON {2} ({3})";

	private String createTableFormat = "CREATE TABLE {0} ({1} {2} {3} {4})";

	private String addColumnFormat = "ALTER TABLE {0} ADD COLUMN {1} {2} {3}";

	private String className;

	private String dbURL;

	private StackTraceElement[] currentTransactionStack;

	private TransactionStarter transactionStarter;

	public Connection getConnection() {
		return connection;
	}

	public void setTransactionStarter(TransactionStarter starter) {
		this.transactionStarter = starter;
	}

	private String quote(String s) {
		return s;
	}

	private void executeWithException(String s) throws SQLException {
		connection.createStatement().execute(s);
	}

	private void createIndex(String table, boolean unique, String... columns) throws SQLException {
		StringBuffer columnNames = new StringBuffer();
		StringBuffer indexName = new StringBuffer(table).append("_");
		for (int i = 0; i < columns.length; i++) {
			columnNames.append(columns[i]);
			indexName.append(columns[i]);
			if (i < columns.length - 1) {
				columnNames.append(", ");
				indexName.append("_");
			}
		}
		executeWithException(MessageFormat.format(createIndexFormat, unique ? "UNIQUE " : "", quote(indexName.toString()), quote(table),
				quote(columnNames.toString())));
	}

	private void createTable(String table, String column, Class klass, boolean primaryKey, boolean unique) throws SQLException {
		if (primaryKey && unique) {
			throw new RuntimeException("You can not define a primary key as unique. It is unique anyway!");
		}
		executeWithException(MessageFormat.format(createTableFormat, quote(table), quote(column), classMap.get(klass), primaryKey ? "PRIMARY KEY"
				: "", unique ? "UNIQUE" : ""));
	}

	private void addColumn(String table, String column, Class klass, boolean unique) throws SQLException {
		executeWithException(MessageFormat.format(addColumnFormat, quote(table), quote(column), classMap.get(klass), unique ? "UNIQUE" : ""));
	}

	/**
	 * Class used to ensure presence of given indices in the database
	 */
	public class Index {
		private String tableName;

		private String[] columns;

		private boolean unique;

		/**
		 * Create an Index instance
		 * 
		 * @param tableName
		 *            the tableName
		 * @param columns
		 *            the columns to cover
		 */
		public Index(String tableName, String... columns) {
			this.tableName = tableName;
			this.columns = columns;
			this.unique = false;
		}

		/**
		 * Make the index unique
		 */
		public Index unique() {
			unique = true;
			return this;
		}

		public void ensure() {
			try {
				DatabaseMetaData metaData = connection.getMetaData();
				ResultSet tableIndices = metaData.getIndexInfo(null, null, tableName, unique, false);
				Map<String, Set<String>> indices = new HashMap<String, Set<String>>();
				while (tableIndices.next()) {
					if (tableIndices.getBoolean("NON_UNIQUE") == !unique) {
						String indexName = tableIndices.getString("INDEX_NAME");
						Set<String> columns = indices.get(indexName);
						if (columns == null) {
							columns = new HashSet<String>();
							indices.put(indexName, columns);
						}
						columns.add(tableIndices.getString("COLUMN_NAME"));
					}
				}
				tableIndices.close();
				boolean hasIndex = false;
				Set<String> wantedColumns = new HashSet<String>(Arrays.asList(columns));
				for (Map.Entry<String, Set<String>> entry : indices.entrySet()) {
					if (entry.getValue().equals(wantedColumns)) {
						hasIndex = true;
					}
				}
				if (!hasIndex) {
					createIndex(tableName, unique, columns);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Class used to ensure presence of given columns in the database;
	 */
	public class Column {
		private String tableName;

		private String columnName;

		private Class klass;

		private boolean indexed;

		private boolean primaryKey;

		private boolean unique;

		/**
		 * Create a Column instance.
		 * 
		 * @param tn
		 *            the table name
		 * @param cn
		 *            the column name
		 * @param c
		 *            the Class to provide a column for
		 */
		public Column(String tn, String cn, Class c) {
			tableName = tn;
			columnName = cn;
			klass = c;
			indexed = false;
			primaryKey = false;
			unique = false;
		}

		/**
		 * Make the Column indexed.
		 * 
		 * @return this Column
		 */
		public Column indexed() {
			if (primaryKey || unique) {
				throw new RuntimeException("A " + this.getClass().getName() + " that is already unique or primaryKey can not be indexed");
			}
			indexed = true;
			return this;
		}

		/**
		 * Make the Column into a primary key column.
		 * 
		 * @return this Column
		 */
		public Column primaryKey() {
			if (indexed || unique) {
				throw new RuntimeException("A " + this.getClass().getName() + " that is already indexed or unique can not be primaryKey");
			}
			primaryKey = true;
			return this;
		}

		/**
		 * Make the Column unique.
		 * 
		 * @return this Column
		 */
		public Column unique() {
			if (primaryKey || indexed) {
				throw new RuntimeException("A " + this.getClass().getName() + " that is already primaryKey or indexed can not be unique");
			}
			unique = true;
			return this;
		}

		/**
		 * Ensure that the column this Column describes exists in the database.
		 */
		public void ensure() {
			try {
				DatabaseMetaData metaData = connection.getMetaData();
				ResultSet matchingTables = metaData.getTables(null, null, tableName, null);
				if (matchingTables.next()) {
					matchingTables.close();
					ResultSet matchingColumns = metaData.getColumns(null, null, tableName, columnName);
					if (matchingColumns.next()) {
						if (indexed || unique) {
							Index index = new Index(tableName, columnName);
							if (unique) {
								index.unique();
							}
							index.ensure();
						}
						matchingColumns.close();
					} else {
						matchingColumns.close();
						addColumn(tableName, columnName, klass, unique);
						if (indexed) {
							new Index(tableName, columnName).ensure();
						}
					}
				} else {
					matchingTables.close();
					createTable(tableName, columnName, klass, primaryKey, unique);
					if (indexed) {
						new Index(tableName, columnName).ensure();
					}
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * A class that provides JDBC instances on a per thread basis.
	 */
	public static class JDBCPool extends Pool<JDBC> {
		private String className;

		private String uri;

		private Properties properties;

		private TransactionStarter transactionStarter;

		public JDBCPool(String c, String u, Properties p) {
			className = c;
			uri = u;
			properties = p;
		}

		public JDBCPool(String c, String u) {
			this(c, u, null);
		}

		public void setTransactionStarter(TransactionStarter starter) {
			this.transactionStarter = starter;
		}

		public JDBC create() {
			JDBC returnValue = new JDBC(className, uri, properties);
			returnValue.setTransactionStarter(transactionStarter);
			return returnValue;
		}

		/**
		 * Get a Dumper able to dump instances of the given class.
		 * 
		 * @param klass
		 *            the class the Dumper should be able to dump into this jdbc connection
		 * @return a Dumper instance able to dump instances of the given class
		 */
		public Dumper getDumper(Class<? extends Object> klass) {
			if (!className.equals("org.postgresql.Driver")) {
				throw new RuntimeException("" + this.getClass().getName() + "#getDumper is only implemented for org.postgresql.Driver connections");
			}

			JDBC jdbc = acquire();
			try {
				final String tableName = getTableName(klass);
				Collection<String> fields = new ArrayList<String>();
				Dumper.IdGetter idGetter = null;
				String idField = null;
				DatabaseMetaData metaData = jdbc.getMetaData();
				ResultSet matchingTables = metaData.getTables(null, null, tableName, null);
				if (matchingTables.next()) {
					matchingTables.close();
					ResultSet matchingColumns = metaData.getColumns(null, null, tableName, null);
					while (matchingColumns.next()) {
						String columnName = (String) matchingColumns.getObject("COLUMN_NAME");
						if ("YES".equals(matchingColumns.getObject("IS_AUTOINCREMENT"))) {
							String camelizedField = capitalize(camelize(columnName));
							idField = columnName;
							idGetter = new Dumper.IdGetter() {
								public Long get(JDBC j) {
									return j.query(Long.class, "currval", "SELECT currval('" + tableName + "_id_seq')");
								}
							};
						} else {
							fields.add(columnName);
						}
					}
					matchingColumns.close();
					return new Dumper(this, klass, tableName, fields, idField, idGetter);
				} else {
					matchingTables.close();
					throw new RuntimeException("No table " + tableName + " found");
				}
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				release();
			}
		}

		/**
		 * Get a Loader able to load instances of the given class.
		 * 
		 * @param klass
		 *            the class the Loader should be able to load from this jdbc connection
		 * @return a Loader instance able to load instances of the given class
		 */
		public <T> Loader<T> getLoader(Class<T> klass) {
			if (!className.equals("org.postgresql.Driver")) {
				throw new RuntimeException("" + this.getClass().getName() + "#getLoader is only implemented for org.postgresql.Driver connections");
			}

			try {
				JDBC jdbc = acquire();
				String tableName = getTableName(klass);
				String idField = null;
				Collection<String> fields = new ArrayList<String>();
				DatabaseMetaData metaData = jdbc.getMetaData();
				ResultSet matchingTables = metaData.getTables(null, null, tableName, null);
				if (matchingTables.next()) {
					matchingTables.close();
					ResultSet matchingColumns = metaData.getColumns(null, null, tableName, null);
					while (matchingColumns.next()) {
						String columnName = (String) matchingColumns.getObject("COLUMN_NAME");
						if ("YES".equals(matchingColumns.getObject("IS_AUTOINCREMENT"))) {
							idField = columnName;
						}
						fields.add(columnName);
					}
					matchingColumns.close();
					return new Loader<T>(this, klass, tableName, fields, idField);
				} else {
					matchingTables.close();
					throw new RuntimeException("No table " + tableName + " found");
				}
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				release();
			}

		}
	}

	/**
	 * Create a JDBC wrapper for the given URL.
	 * 
	 * @param className
	 *            a name of a class to make sure is loaded - for example the class of the driver
	 * @param dbURL
	 *            the URL of the database to connect to
	 */
	public JDBC(String c, String u) {
		this(c, u, null);
	}

	/**
	 * Create a JDBC wrapper for the given URL.
	 * 
	 * @param className
	 *            a name of a class to make sure is loaded - for example the class of the driver
	 * @param dbURL
	 *            the URL of the database to connect to
	 * @param p
	 *            the Properties to use when setting up the connection
	 */
	public JDBC(String c, String u, Properties p) {
		className = c;
		dbURL = u;
		try {
			Class.forName(className);
			if (p == null) {
				connection = DriverManager.getConnection(dbURL);
			} else {
				connection = DriverManager.getConnection(dbURL, p);
			}
			Thread shutdownThread = new Thread() {
				public void run() {
					JDBC.this.close();
				}
			};
			shutdownThread.setUncaughtExceptionHandler(new DefaultUncaughtExceptionHandler());
			Runtime.getRuntime().addShutdownHook(shutdownThread);
			makeClassMap();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Return a Column object able to ensure that a defined sql column exists in the database;
	 * 
	 * @param tableName
	 *            the name of the table the column should belong to
	 * @param columnName
	 *            the name of the column
	 * @param klass
	 *            the Class the column should accomodate
	 * @return a new Column object matching the given parameters
	 */
	public Column col(String tableName, String columnName, Class klass) {
		return new Column(tableName, columnName, klass);
	}

	/**
	 * Return an Index object able to ensure that a defined index exists in the database.
	 * 
	 * @param tableName
	 *            the name of the table the index should belong to
	 * @param columns
	 *            the names of the columns the index should belong to
	 */
	public Index ind(String tableName, String... columns) {
		return new Index(tableName, columns);
	}

	/**
	 * @return the class name of the jdbc driver this JDBC wraps
	 */
	public String getClassName() {
		return className;
	}

	/**
	 * @return the jdbc url this JDBC connects to
	 */
	public String getDbURL() {
		return dbURL;
	}

	protected DatabaseMetaData getMetaData() {
		try {
			return connection.getMetaData();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void makeClassMap() {
		if (className.equals("org.hsqldb.jdbcDriver") || className.equals("org.hsqldb.jdbc.JDBCDriver")) {
			classMap = new HashMap<Class, String>();
			classMap.put(String.class, "LONGVARCHAR");
			classMap.put(byte[].class, "LONGVARBINARY");
			classMap.put(Integer.class, "INTEGER");
			classMap.put(Long.class, "BIGINT");
			classMap.put(Double.class, "DOUBLE");
			classMap.put(Float.class, "REAL");
			classMap.put(Boolean.class, "BIT");
		} else if (className.equals("org.h2.Driver")) {
			classMap = new HashMap<Class, String>();
			classMap.put(String.class, "LONGVARCHAR");
			classMap.put(byte[].class, "LONGVARBINARY");
			classMap.put(Integer.class, "INTEGER");
			classMap.put(Long.class, "BIGINT");
			classMap.put(Double.class, "DOUBLE");
			classMap.put(Float.class, "REAL");
			classMap.put(Boolean.class, "BIT");
		} else if (className.equals("org.postgresql.Driver")) {
			classMap = new HashMap<Class, String>();
			classMap.put(String.class, "TEXT");
			classMap.put(byte[].class, "BYTEA");
			classMap.put(Integer.class, "INT4");
			classMap.put(Long.class, "INT8");
			classMap.put(Double.class, "DOUBLE PRECISION");
			classMap.put(Float.class, "REAL");
			classMap.put(Boolean.class, "BOOLEAN");
			classMap.put(Serial.class, "SERIAL");
			classMap.put(Timestamp.class, "TIMESTAMP");
			classMap.put(java.sql.Date.class, "DATE");
		}
	}

	/**
	 * Close the underlying connection.
	 */
	public void close() {
		try {
			connection.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Execute the given sql query.
	 * 
	 * @param s
	 *            the query to execute
	 * @return the number of affected rows
	 */
	public int execute(String s) {
		return execute(s, new Object[0]);
	}

	/**
	 * Begin a transaction.
	 * 
	 * No operations after this will be commited until the commit method is called.
	 */
	public void begin() {
		try {
			if (inTransaction()) {
				throw new RuntimeException("A transaction is already under way from " + Arrays.asList(currentTransactionStack));
			}
			currentTransactionStack = new RuntimeException().getStackTrace();
			connection.setAutoCommit(false);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Get if we are in a transaction.
	 * 
	 * @return true if we are in a transaction
	 */
	public boolean inTransaction() {
		try {
			return !connection.getAutoCommit();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Commit a transaction.
	 * 
	 * All operations between the last begin and this commit will be commited.
	 */
	public void commit() {
		try {
			connection.commit();
			connection.setAutoCommit(true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Rollback a transaction.
	 * 
	 * All operations between the last begin and this rollback will be cancelled.
	 */
	public void rollback() {
		try {
			connection.rollback();
			connection.setAutoCommit(true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Executes the given query with a whole bunch of arguments.
	 * 
	 * @param s
	 *            the query to execute
	 * @param argumentSet
	 *            all argument arrays we want to execute the query with
	 * @return all the result codes from the executions
	 */
	public int[] executeBatch(String s, Collection<Object[]> argumentSet) {
		if (argumentSet.size() == 0) {
			return new int[] {};
		}
		try {
			int[] returnValue;
			boolean oldAutoCommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			PreparedStatement statement = connection.prepareStatement(s);
			Iterator<Object[]> argumentIterator = argumentSet.iterator();
			while (argumentIterator.hasNext()) {
				insertIntoStatement(statement, argumentIterator.next());
				statement.addBatch();
			}
			returnValue = statement.executeBatch();
			statement.close();
			connection.setAutoCommit(oldAutoCommit);
			return returnValue;
		} catch (Exception e) {
			throw new RuntimeException("While doing executeBatch(" + s + ", " + argumentSet + ")", e);
		}
	}

	private void insertIntoStatement(PreparedStatement statement, Object[] arguments) throws SQLException {
		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i] instanceof byte[]) {
				statement.setBytes(i + 1, (byte[]) arguments[i]);
			} else {
				statement.setObject(i + 1, arguments[i]);
			}
		}
	}

	/**
	 * Execute the given sql query with arguments.
	 * 
	 * @param s
	 *            the query to execute
	 * @param arguments
	 *            the arguments to replace the ?'s in the query with
	 * @return the number of affected rows
	 */
	public int execute(String s, Object... arguments) {
		try {
			int returnValue;
			PreparedStatement statement = connection.prepareStatement(s);
			insertIntoStatement(statement, arguments);
			returnValue = statement.executeUpdate();
			statement.close();
			return returnValue;
		} catch (Exception e) {
			throw new RuntimeException("While doing execute(" + s + ", " + Arrays.asList(arguments) + ")", e);
		}
	}

	/**
	 * Execute the given sql query.
	 * 
	 * @param s
	 *            the query to execute
	 * @return the results of the query
	 */
	public ResultSetWrapper query(String s) {
		return query(s, new Object[0]);
	}

	/**
	 * Execute the given query and return a column of the first result.
	 * 
	 * @param klass
	 *            the class of object to return
	 * @param col
	 *            the name of the column to return
	 * @param s
	 *            the query to execute
	 * @param arguments
	 *            the arguments to the query
	 * @return the wanted column of the wanted result set, or null if no results were gotten
	 */
	public <T> T query(Class<T> klass, String col, String s, Object... arguments) {
		ResultSetWrapper result = query(s, arguments);
		if (result.next()) {
			T returnValue = result.get(klass, col);
			result.close();
			return returnValue;
		} else {
			return null;
		}
	}

	/**
	 * Execute the given sql query with arguments.
	 * 
	 * @param s
	 *            the query to execute
	 * @param arguments
	 *            the arguments to replace the ?'s in the query with
	 * @return the results of the query
	 */
	public ResultSetWrapper query(String s, Object... arguments) {
		try {
			PreparedStatement statement = connection.prepareStatement(s);
			insertIntoStatement(statement, arguments);
			return new ResultSetWrapper(statement.executeQuery());
		} catch (Exception e) {
			throw new RuntimeException("While doing query(" + s + ", " + Arrays.asList(arguments) + ")", e);
		}
	}

	/**
	 * Execute the callable within a transaction.
	 * 
	 * @param callable
	 *            the Callable to execute
	 * @return the return value of the Callable
	 */
	public <T> T transaction(Callable<T> callable) {
		if (callable instanceof JDBCCallable) {
			((JDBCCallable) callable).setJDBC(this);
		}
		begin();
		try {
			if (transactionStarter != null) {
				transactionStarter.start(this);
			}
			T returnValue = callable.call();
			commit();
			return returnValue;
		} catch (RuntimeException e) {
			rollback();
			throw e;
		} catch (Exception e) {
			rollback();
			throw new RuntimeException(e);
		}
	}

}