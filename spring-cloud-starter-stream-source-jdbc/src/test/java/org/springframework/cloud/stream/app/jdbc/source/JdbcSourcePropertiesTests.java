/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.app.jdbc.source;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * @author Thomas Risberg
 * @author Artem Bilan
 */
public class JdbcSourcePropertiesTests {

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
	public void queryIsRequired() {
		this.thrown.expect(BeanCreationException.class);
		this.thrown.expectMessage("Failed to bind properties under 'jdbc' to org.springframework.cloud.stream.app.jdbc.source.JdbcSourceProperties");
		this.context.register(Conf.class);
		this.context.refresh();
	}

	@Test
	public void queryCanBeCustomized() {
		String query = "select foo from bar";
		TestPropertyValues.of("jdbc.query:" + query)
				.applyTo(this.context);
		this.context.register(Conf.class);
		this.context.refresh();
		JdbcSourceProperties properties = this.context.getBean(JdbcSourceProperties.class);
		assertThat(properties.getQuery(), equalTo(query));
	}

	@Test
	public void updateCanBeCustomized() {
		String update = "update bar set baz=1 where foo in (:foo)";
		TestPropertyValues.of("jdbc.query:select foo from bar where baz < 1", "jdbc.update:" + update)
				.applyTo(this.context);
		this.context.register(Conf.class);
		this.context.refresh();
		JdbcSourceProperties properties = this.context.getBean(JdbcSourceProperties.class);
		assertThat(properties.getUpdate(), equalTo(update));
	}

	@Test
	public void splitDefaultsToTrue() {
		TestPropertyValues.of("jdbc.query:select foo from bar")
				.applyTo(this.context);
		this.context.register(Conf.class);
		this.context.refresh();
		JdbcSourceProperties properties = this.context.getBean(JdbcSourceProperties.class);
		assertThat(properties.isSplit(), equalTo(true));
	}

	@Test
	public void splitCanBeCustomized() {
		TestPropertyValues.of("jdbc.query:select foo from bar", "jdbc.split:false")
				.applyTo(this.context);
		this.context.register(Conf.class);
		this.context.refresh();
		JdbcSourceProperties properties = this.context.getBean(JdbcSourceProperties.class);
		assertThat(properties.isSplit(), equalTo(false));
	}

	@Test
	public void maxRowsPerPollCanBeCustomized() {
		TestPropertyValues.of("jdbc.query:select foo from bar", "jdbc.maxRowsPerPoll:15")
				.applyTo(this.context);
		this.context.register(Conf.class);
		this.context.refresh();
		JdbcSourceProperties properties = this.context.getBean(JdbcSourceProperties.class);
		assertThat(properties.getMaxRowsPerPoll(), equalTo(15));
	}

	@Configuration
	@EnableConfigurationProperties(JdbcSourceProperties.class)
	static class Conf {

	}

}
