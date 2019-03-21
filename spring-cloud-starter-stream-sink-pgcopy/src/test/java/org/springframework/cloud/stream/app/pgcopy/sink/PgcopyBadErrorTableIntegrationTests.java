/*
 * Copyright 2017 the original author or authors.
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

import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.app.pgcopy.test.PostgresTestSupport;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Integration Tests testing bad error table specified for PgcopySink. Only runs if PostgreSQL database is available.
 *
 * @author Thomas Risberg
 * @author Artem Bilan
 */
public class PgcopyBadErrorTableIntegrationTests {

	@ClassRule
	public static PostgresTestSupport postgresAvailable = new PostgresTestSupport();

	private String[] env = { "pgcopy.tableName=names", "pgcopy.columns=id,name,age", "pgcopy.format=CSV" };

	private String[] jdbc = { };

	private Properties appProperties = new Properties();

	@Before
	public void setup() {
		try {
			appProperties = PropertiesLoaderUtils.loadAllProperties("application.properties");
		}
		catch (IOException e) {
		}
		List<String> jdbcProperties = new ArrayList<>();
		for (Object key : appProperties.keySet()) {
			jdbcProperties.add(key + "=" + appProperties.get(key));
		}
		this.jdbc = jdbcProperties.toArray(new String[0]);
	}

	@Test
	public void testBadErrorTableName() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of(this.jdbc)
				.and("pgcopy.error-table=missing")
				.and(this.env)
				.applyTo(context);
		context.register(PgcopySinkApplication.class);
		try {
			context.refresh();
		}
		catch (Exception e) {
			Throwable ise = null;
			Throwable dae = null;
			Throwable cause = e;
			while (cause.getCause() != null) {
				cause = cause.getCause();
				if (cause instanceof IllegalStateException) {
					ise = cause;
				}
				if (cause instanceof DataAccessException) {
					dae = cause;
				}
			}
			Assert.assertThat(cause.getClass().getName(), is(PSQLException.class.getName()));
			Assert.assertNotNull(ise);
			Assert.assertTrue(ise.getMessage().contains("Invalid error table specified"));
			Assert.assertNotNull(dae);
			Assert.assertTrue(cause.getMessage().contains("relation"));
			Assert.assertTrue(cause.getMessage().contains("does not exist"));
		}
		context.close();
	}

	@Test
	public void testBadErrorTableFields() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(appProperties.getProperty("spring.datasource.driver-class-name"));
		dataSource.setUrl(appProperties.getProperty("spring.datasource.url"));
		dataSource.setUsername(appProperties.getProperty("spring.datasource.username"));
		dataSource.setPassword(appProperties.getProperty("spring.datasource.password"));
		JdbcOperations jdbcOperations = new JdbcTemplate(dataSource);
		try {
			jdbcOperations.execute(
					"drop table test_errors");
		}
		catch (Exception e) {
		}
		try {
			jdbcOperations.execute(
					"create table test_errors (table_name varchar(255), error_message text)");
		}
		catch (Exception e) {
			throw new IllegalStateException("Error creating table", e);
		}
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of(this.jdbc)
				.and("pgcopy.error-table=test_errors")
				.and(this.env)
				.applyTo(context);
		context.register(PgcopySinkApplication.class);
		try {
			context.refresh();
		}
		catch (Exception e) {
			Throwable ise = null;
			Throwable dae = null;
			Throwable cause = e;
			while (cause.getCause() != null) {
				cause = cause.getCause();
				if (cause instanceof IllegalStateException) {
					ise = cause;
				}
				if (cause instanceof DataAccessException) {
					dae = cause;
				}
			}
			Assert.assertThat(cause.getClass().getName(), is(PSQLException.class.getName()));
			Assert.assertNotNull(ise);
			Assert.assertTrue(ise.getMessage().contains("Invalid error table specified"));
			Assert.assertNotNull(dae);
			Assert.assertTrue(cause.getMessage().contains("column"));
			Assert.assertTrue(cause.getMessage().contains("does not exist"));
		}
		context.close();
	}

	@SpringBootApplication
	public static class PgcopySinkApplication {

		public static void main(String[] args) {
			SpringApplication.run(PgcopySinkApplication.class, args);
		}

	}

}
