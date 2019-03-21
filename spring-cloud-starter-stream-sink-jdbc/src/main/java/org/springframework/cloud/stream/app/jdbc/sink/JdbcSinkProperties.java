/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.jdbc.sink;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.app.jdbc.ShorthandMapConverter;


/**
 * Holds configuration properties for the Jdbc Sink module.
 *
 * @author Eric Bottard
 * @author Artem Bilan
 */
@ConfigurationProperties("jdbc")
public class JdbcSinkProperties {

	@Autowired
	private ShorthandMapConverter shorthandMapConverter;

	/**
	 * The name of the table to write into.
	 */
	private String tableName = "messages";

	/**
	 * The comma separated colon-based pairs of column names and SpEL expressions for values to insert/update.
	 * Names are used at initialization time to issue the DDL.
	 */
	private String columns = "payload:payload.toString()";

	/**
	 * 'true', 'false' or the location of a custom initialization script for the table.
	 */
	private String initialize = "false";

	private Map<String, String> columnsMap;

	public String getTableName() {
		return this.tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumns() {
		return this.columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getInitialize() {
		return this.initialize;
	}

	public void setInitialize(String initialize) {
		this.initialize = initialize;
	}

	Map<String, String> getColumnsMap() {
		if (this.columnsMap == null) {
			this.columnsMap = this.shorthandMapConverter.convert(this.columns);
		}
		return this.columnsMap;
	}

}
