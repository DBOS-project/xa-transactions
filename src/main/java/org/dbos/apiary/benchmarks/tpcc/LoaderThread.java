/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package org.dbos.apiary.benchmarks.tpcc;

import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.SQLException;

public abstract class LoaderThread implements Runnable {
	private static final Logger LOG = Logger.getLogger(LoaderThread.class);
	private final Connection conn;
	private final String databaseName;
	
	public LoaderThread(Connection connection, String databaseName) throws SQLException {
		this.conn = connection;
		this.conn.setAutoCommit(false);
		this.databaseName = databaseName;
	}
	
	@Override
	public final void run() {
		try {
			this.load(this.conn);
			this.conn.commit();
		} catch (SQLException ex) {
			SQLException next_ex = ex.getNextException();
			String msg = String.format("Unexpected error when loading %s database", databaseName);
			LOG.error(msg, next_ex);
			throw new RuntimeException(ex);
		}
	}

	/**
	 * This is the method that each LoaderThread has to implement
	 * @param conn
	 * @throws SQLException
	 */
	public abstract void load(Connection conn) throws SQLException;

	public Connection getConnection() {
		return conn;
	}
	
}