/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.sql.ResultSet;

/**
 * A very simple wrapper for the ResultSet in java.sql.
 */
public class ResultSetWrapper {
	private ResultSet resultSet;

	/**
	 * Create a wrapper.
	 * 
	 * @param r
	 *            the ResultSet to wrap
	 */
	public ResultSetWrapper(ResultSet r) {
		resultSet = r;
	}

	/**
	 * Get a column from the backing ResultSet.
	 * 
	 * @param column
	 *            the column name that we want to retrieve from
	 * @return the object in the given column
	 */
	public Object get(String column) {
		try {
			return resultSet.getObject(column);
		} catch (Exception e) {
			close();
			throw new RuntimeException(e);
		}
	}

	/**
	 * Get a column of a given class from the backing ResultSet.
	 * 
	 * @param klass
	 *            the class of the object we want to retrieve
	 * @param column
	 *            the column name that we want to retrieve from
	 * @return the object in the given column
	 */
	@SuppressWarnings("unchecked")
	public <T> T get(Class<T> klass, String column) {
		try {
			if (klass == byte[].class) {
				return (T) resultSet.getBytes(column);
			} else {
				return (T) resultSet.getObject(column);
			}
		} catch (Exception e) {
			close();
			throw new RuntimeException(e);
		}
	}

	/**
	 * Close this wrapper.
	 */
	public void close() {
		try {
			resultSet.getStatement().close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Step to the next (or first, if the wrapper is newly created) row in the result.
	 * 
	 * @return whether there is a row to retrieve
	 */
	public boolean next() {
		try {
			boolean returnValue = resultSet.next();
			if (!returnValue) {
				close();
			}
			return returnValue;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Will close the wrapped ResultSet.
	 */
	protected void finalize() {
		close();
	}
}
