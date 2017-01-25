/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.pgcopy.sink;

import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.app.jdbc.SupportsShorthands;

import java.util.Collections;
import java.util.List;

/**
 * Used to configure the pgcopy sink module options that are related to writing using the PostgreSQL CopyManager API.
 *
 * @author Thomas Risberg
 */
@SuppressWarnings("unused")
@ConfigurationProperties("pgcopy")
public class PgcopySinkProperties {

	/**
	 * The name of the table to write into.
	 */
	@NotNull
	private String tableName;

	/**
	 * The names of the columns that shall receive data.
	 * Also used at initialization time to issue the DDL.
	 */
	private List<String> columns = Collections.singletonList("payload");

	/**
	 * Threshold in number of messages when data will be flushed to database table.
	 */
	private int batchSize = 10000;

	/**
	 * Idle timeout in milliseconds when data is automatically flushed to database table.
	 */
	private long idleTimeout = -1L;

	/**
	 * 'true', 'false' or the location of a custom initialization script for the table.
	 */
	private String initialize = "false";

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getColumns() {
		return columns;
	}

	@SupportsShorthands
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public String getInitialize() {
		return initialize;
	}

	public void setInitialize(String initialize) {
		this.initialize = initialize;
	}
}