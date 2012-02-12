/*
 * This file is part of Nja. Nja is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. Nja is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details. You should have received a copy of the GNU General Public License along with Nja. If not, see
 * <http://www.gnu.org/licenses/>. Copyright 2009 Martin Kihlgren <zond at troja dot ath dot cx>
 */

package cx.ath.troja.nja;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigInteger;
import java.security.MessageDigest;

public class Checksum {

	public static String md5(String data) {
		try {
			MessageDigest sha = MessageDigest.getInstance("MD5");
			return new BigInteger(1, sha.digest(data.getBytes("UTF-8"))).toString(16);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static String hex(byte[] bytes) {
		return new BigInteger(1, bytes(bytes)).toString(16);
	}

	public static String hex(String s) {
		try {
			return hex(s.getBytes("UTF-8"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static String hex(File file) {
		try {
			byte[] buffer = new byte[8192];
			FileInputStream input = new FileInputStream(file);
			int read = 0;
			MessageDigest sha = MessageDigest.getInstance("SHA");
			while ((read = input.read(buffer)) != -1) {
				sha.update(buffer, 0, read);
			}
			input.close();
			return hex(sha.digest());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static byte[] bytes(byte[] bytes) {
		try {
			MessageDigest sha = MessageDigest.getInstance("SHA");
			return sha.digest(bytes);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static BigInteger sum(byte[] bytes) {
		return new BigInteger(1, bytes(bytes));
	}

}