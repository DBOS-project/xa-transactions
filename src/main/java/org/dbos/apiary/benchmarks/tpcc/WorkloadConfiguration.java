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

import java.sql.Connection;

public class WorkloadConfiguration {
    
	//private DatabaseType db_type;
	private String benchmarkName;
	public String getBenchmarkName() {
        return benchmarkName;
    }

    public void setBenchmarkName(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    private String db_connection;
	private String db_address_mysql;
	private String db_address_pg;
	private String db_name;
	private String db_username;
	private String db_password;
	private String db_driver;	
	private double scaleFactor = 1.0;
	private double selectivity = -1.0;
	private int numWarehouses;
	private int loaderThreads = ThreadUtil.availableProcessors();
	private int numTxnTypes;



    private int numberOfPhases = 0;
	private int isolationMode = Connection.TRANSACTION_SERIALIZABLE;
	private boolean recordAbortMessages = false;
    private String dataDir = null;

	public void setDBConnection(String database) {
		this.db_connection = database;
	}
	
	public String getDBConnection() {
		return db_connection;
	}


	public void setDBAddressMySQL(String mysqlAddr) {
		this.db_address_mysql = mysqlAddr;
	}
	
	public String getDBAddressMySQL() {
		return db_address_mysql;
	}

	public void setDBAddressPG(String postgresAddr) {
		this.db_address_pg = postgresAddr;
	}
	
	public String getDBAddressPG() {
		return db_address_pg;
	}
	
	public void setDBName(String dbname) {
		this.db_name = dbname;
	}
	
	public void setLoaderThreads(int loaderThreads) {
        this.loaderThreads = loaderThreads;
    }
	
	/**
	 * The number of loader threads that the framework is allowed to use.
	 * @return
	 */
	public int getLoaderThreads() {
        return this.loaderThreads;
    }
	
	public int getNumTxnTypes() {
		return numTxnTypes;
	}
	
	public void setNumTxnTypes(int numTxnTypes) {
		this.numTxnTypes = numTxnTypes;
	}
	
	public String getDBName() {
		return db_name;
	}

	public void setDBUsername(String username) {
		this.db_username = username;
	}
	
	public String getDBUsername() {
		return db_username;
	}

	public void setDBPassword(String password) {
		this.db_password = password;
	}
	
	public String getDBPassword() {
		return this.db_password;
	}

	public void setSelectivity(double selectivity) {
        this.selectivity = selectivity;
    }
	
	public double getSelectivity() {
	    return this.selectivity;
	}

	public void setDBDriver(String driver) {
		this.db_driver = driver;
	}
	
	public String getDBDriver() {
		return this.db_driver;
	}
	
	public void setRecordAbortMessages(boolean recordAbortMessages) {
        this.recordAbortMessages = recordAbortMessages;
    }
	
	/**
	 * Whether each worker should record the transaction's UserAbort messages
	 * This primarily useful for debugging a benchmark
	 */
	public boolean getRecordAbortMessages() {
        return (this.recordAbortMessages);
    }
	
	/**
	 * Set the scale factor for the database
	 * A value of 1 means the default size.
	 * A value greater than 1 means the database is larger
	 * A value less than 1 means the database is smaller 
	 * @param scaleFactor
	 */
	public void setScaleFactor(double scaleFactor) {
        this.scaleFactor = scaleFactor;
    }
	/**
	 * Return the scale factor of the database size
	 * @return
	 */
	public double getScaleFactor() {
        return this.scaleFactor;
    }

	/**
	 * Return the number of phases specified in the config file
	 * @return
	 */
	public int getNumberOfPhases() {
		return this.numberOfPhases;
	}

	/**
     * Set the directory in which we can find the data files (for example, CSV
     * files) for loading the database.
     */ 
    public void setDataDir(String dir) {
        this.dataDir = dir;
    }

    /**
     * Return the directory in which we can find the data files (for example, CSV
     * files) for loading the database.
     */ 
    public String getDataDir() {
        return this.dataDir;
    }

    /**
	 * A utility method that init the phaseIterator and dialectMap
	 */
	public void init() {
	    try {
	        Class.forName(this.db_driver);
	    } catch (ClassNotFoundException ex) {
	        throw new RuntimeException("Failed to initialize JDBC driver '" + this.db_driver + "'", ex);
	    }
	}

	public void setNumWarehouses(int numWarehouses) {
		this.numWarehouses = numWarehouses;
	}

	public int getNumWarehouses() {
		return numWarehouses;
	}
	
	public int getIsolationMode() {
		return isolationMode;
	}

    public String getIsolationString() {
        if(this.isolationMode== Connection.TRANSACTION_SERIALIZABLE)
            return "TRANSACTION_SERIALIZABLE";
        else if(this.isolationMode==Connection.TRANSACTION_READ_COMMITTED)
            return "TRANSACTION_READ_COMMITTED";
        else if(this.isolationMode==Connection.TRANSACTION_REPEATABLE_READ)
            return "TRANSACTION_REPEATABLE_READ";
        else if(this.isolationMode==Connection.TRANSACTION_READ_UNCOMMITTED)
            return "TRANSACTION_READ_UNCOMMITTED";
        else
            return "TRANSACTION_SERIALIZABLE [DEFAULT]";
    }

	public void setIsolationMode(String mode) {
		if(mode.equals("TRANSACTION_SERIALIZABLE"))
			this.isolationMode= Connection.TRANSACTION_SERIALIZABLE;
		else if(mode.equals("TRANSACTION_READ_COMMITTED"))
			this.isolationMode=Connection.TRANSACTION_READ_COMMITTED;
		else if(mode.equals("TRANSACTION_REPEATABLE_READ"))
			this.isolationMode=Connection.TRANSACTION_REPEATABLE_READ;
		else if(mode.equals("TRANSACTION_READ_UNCOMMITTED"))
			this.isolationMode=Connection.TRANSACTION_READ_UNCOMMITTED;
		else if(!mode.isEmpty())
			System.out.println("Indefined isolation mode, set to default [TRANSACTION_SERIALIZABLE]");
	}
}
