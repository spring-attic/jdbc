/*
 * Copyright 2015 the original author or authors.
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

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.pgcopy.test.PostgresTestSupport;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.*;

/**
 * Integration Tests for PgcopySink. Only runs if PostgreSQL database is available.
 *
 * @author Thomas Risberg
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
		classes = PgcopySinkIntegrationTests.PgcopySinkApplication.class)
public abstract class PgcopySinkIntegrationTests {

	@ClassRule
	public static PostgresTestSupport postgresAvailable = new PostgresTestSupport();

	@Autowired
	protected Sink channels;

	@Autowired
	protected JdbcOperations jdbcOperations;

	@TestPropertySource(properties = {"pgcopy.table-name=test", "pgcopy.batch-size=1", "pgcopy.initialize=true"})
	public static class BasicPayloadCopyTests extends PgcopySinkIntegrationTests {

		@Test
		public void testBasicCopy() {
			String sent = "hello42";
			channels.input().send(MessageBuilder.withPayload(sent).build());
			String result = jdbcOperations.queryForObject("select payload from test", String.class);
			Assert.assertThat(result, is("hello42"));
		}
	}

	@TestPropertySource(properties = {"pgcopy.tableName=names", "pgcopy.batch-size=3", "pgcopy.initialize=true",
			"pgcopy.columns=id,name,age"})
	public static class PgopyCSVTests extends PgcopySinkIntegrationTests {

		@Test
		public void testCopyCSV() {
			channels.input().send(MessageBuilder.withPayload("123, \"Nisse\", 25").build());
			channels.input().send(MessageBuilder.withPayload("124, \"Anna\", 21").build());
			channels.input().send(MessageBuilder.withPayload("125, \"Bubba\", 22").build());
			int result = jdbcOperations.queryForObject("select count(*) from names", Integer.class);
			Assert.assertThat(result, is(3));
		}
	}

	@SpringBootApplication
	public static class PgcopySinkApplication {
		public static void main(String[] args) {
			SpringApplication.run(PgcopySinkApplication.class, args);
		}
	}


}