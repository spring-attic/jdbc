/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cloud.stream.app.pgcopy.sink;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * @author Thomas Risberg
 */
public class PgcopySinkPropertiesTests {

	private AnnotationConfigApplicationContext context;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Before
	public void setUp() {
		this.context = new AnnotationConfigApplicationContext();
	}

	@After
	public void tearDown() {
		this.context.close();
	}

	@Test
	public void tableNameIsRequired() {
		this.thrown.expect(BeanCreationException.class);
		this.thrown.expectMessage("Field error in object 'pgcopy' on field 'tableName': rejected value [null]");
		this.context.register(Conf.class);
		this.context.refresh();
	}

	@Test
	public void tableNameCanBeCustomized() {
		String table = "TEST_DATA";
		EnvironmentTestUtils.addEnvironment(this.context, "pgcopy.table-name:" + table);
		this.context.register(Conf.class);
		this.context.refresh();
		PgcopySinkProperties properties = this.context.getBean(PgcopySinkProperties.class);
		assertThat(properties.getTableName(), equalTo(table));
	}

	@Test
	public void formatDefaultsToText() {
		EnvironmentTestUtils.addEnvironment(this.context, "pgcopy.table-name: test");
		this.context.register(Conf.class);
		this.context.refresh();
		PgcopySinkProperties properties = this.context.getBean(PgcopySinkProperties.class);
		assertThat(properties.getFormat().toString(), equalTo("TEXT"));
	}

	@Test
	public void formatCanBeCustomized() {
		String format = "CSV";
		EnvironmentTestUtils.addEnvironment(this.context, "pgcopy.table-name: test", "pgcopy.format:" + format);
		this.context.register(Conf.class);
		this.context.refresh();
		PgcopySinkProperties properties = this.context.getBean(PgcopySinkProperties.class);
		assertThat(properties.getFormat().toString(), equalTo(format));
	}

	@Test
	public void nullCanBeCustomized() {
		String nullString = "@#$";
		EnvironmentTestUtils.addEnvironment(this.context, "pgcopy.table-name: test", "pgcopy.format: CSV",
				"pgcopy.null-string: " + nullString);
		this.context.register(Conf.class);
		this.context.refresh();
		PgcopySinkProperties properties = this.context.getBean(PgcopySinkProperties.class);
		assertThat(properties.getNullString(), equalTo(nullString));
	}

	@Test
	public void delimiterCanBeCustomized() {
		String delimiter = "|";
		EnvironmentTestUtils.addEnvironment(this.context, "pgcopy.table-name: test", "pgcopy.format: CSV",
				"pgcopy.delimiter: " + delimiter);
		this.context.register(Conf.class);
		this.context.refresh();
		PgcopySinkProperties properties = this.context.getBean(PgcopySinkProperties.class);
		assertThat(properties.getDelimiter(), equalTo(delimiter));
	}

	@Test
	public void quoteCanBeCustomized() {
		String quote = "'";
		EnvironmentTestUtils.addEnvironment(this.context, "pgcopy.table-name: test", "pgcopy.format: CSV",
				"pgcopy.quote: " + quote);
		this.context.register(Conf.class);
		this.context.refresh();
		PgcopySinkProperties properties = this.context.getBean(PgcopySinkProperties.class);
		assertThat(String.valueOf(properties.getQuote()), equalTo(quote));
	}

	@Test
	public void escapeCanBeCustomized() {
		String escape = "~";
		EnvironmentTestUtils.addEnvironment(this.context, "pgcopy.table-name: test", "pgcopy.format: CSV",
				"pgcopy.escape: " + escape);
		this.context.register(Conf.class);
		this.context.refresh();
		PgcopySinkProperties properties = this.context.getBean(PgcopySinkProperties.class);
		assertThat(String.valueOf(properties.getEscape()), equalTo(escape));
	}

	@Configuration
	@EnableConfigurationProperties(PgcopySinkProperties.class)
	static class Conf {
	}
}
