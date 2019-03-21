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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.messaging.Message;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionLikeType;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Integration Tests for JdbcSource. Uses hsqldb as a (real) embedded DB.
 *
 * @author Thomas Risberg
 * @author Artem Bilan
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
		properties = "spring.datasource.url=jdbc:h2:mem:test",
		webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext
public abstract class JdbcSourceIntegrationTests {

	protected final ObjectMapper objectMapper = new ObjectMapper();

	@Autowired
	protected Source source;

	@Autowired
	protected JdbcOperations jdbcOperations;

	@Autowired
	protected MessageCollector messageCollector;

	@TestPropertySource(properties = "jdbc.query=select id, name from test order by id")
	public static class DefaultBehaviorTests extends JdbcSourceIntegrationTests {

		@Test
		public void testExtraction() throws Exception {
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));

			Map<?, ?> payload = this.objectMapper.readValue((String) received.getPayload(), Map.class);

			assertEquals(1, payload.get("ID"));
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));

			payload = this.objectMapper.readValue((String) received.getPayload(), Map.class);

			assertEquals(2, payload.get("ID"));
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));

			payload = this.objectMapper.readValue((String) received.getPayload(), Map.class);

			assertEquals(3, payload.get("ID"));
		}

	}

	@TestPropertySource(properties = { "jdbc.query=select id, name, tag from test where tag is NULL order by id", "jdbc.split=false" })
	public static class SelectAllNoSplitTests extends JdbcSourceIntegrationTests {

		@Test
		public void testExtraction() throws Exception {
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));


			CollectionLikeType valueType = TypeFactory.defaultInstance()
					.constructCollectionLikeType(List.class, Map.class);

			List<Map<?, ?>> payload = this.objectMapper.readValue((String) received.getPayload(), valueType);

			assertEquals(3, payload.size());
			assertEquals(1, payload.get(0).get("ID"));
			assertEquals("John", payload.get(2).get("NAME"));
		}

	}

	@TestPropertySource(properties = { "jdbc.query=select id, name from test order by id", "trigger.fixedDelay=600" })
	public static class SelectAllWithDelayTests extends JdbcSourceIntegrationTests {

		@Test
		public void testExtraction() throws Exception {
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));

			Map<?, ?> payload = this.objectMapper.readValue((String) received.getPayload(), Map.class);

			assertEquals(1, payload.get("ID"));
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));
			payload = this.objectMapper.readValue((String) received.getPayload(), Map.class);
			assertEquals(2, payload.get("ID"));
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));
			payload = this.objectMapper.readValue((String) received.getPayload(), Map.class);
			assertEquals(3, payload.get("ID"));
			// should not wrap around to the beginning since delay is 60
			received = messageCollector.forChannel(source.output()).poll(1, TimeUnit.SECONDS);
			assertNull(received);
		}

	}

	@TestPropertySource(properties = { "jdbc.query=select id, name from test order by id", "trigger.fixedDelay=1" })
	public static class SelectAllWithMinDelayTests extends JdbcSourceIntegrationTests {

		@Test
		public void testExtraction() throws Exception {
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));

			Map<?, ?> payload = this.objectMapper.readValue((String) received.getPayload(), Map.class);

			assertEquals(1, payload.get("ID"));
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));

			payload = this.objectMapper.readValue((String) received.getPayload(), Map.class);

			assertEquals(2, payload.get("ID"));
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));

			payload = this.objectMapper.readValue((String) received.getPayload(), Map.class);

			assertEquals(3, payload.get("ID"));
			// should wrap around to the beginning
			received = messageCollector.forChannel(source.output()).poll(2, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));

			payload = this.objectMapper.readValue((String) received.getPayload(), Map.class);

			assertEquals(1, payload.get("ID"));
		}

	}

	@TestPropertySource(properties = {
			"jdbc.query=select id, name, tag from test where tag is NULL order by id",
			"jdbc.split=false",
			"jdbc.maxRowsPerPoll=2",
			"jdbc.update=update test set tag='1' where id in (:id)" })
	public static class Select2PerPollNoSplitWithUpdateTests extends JdbcSourceIntegrationTests {

		@Test
		public void testExtraction() throws Exception {
			Message<?> received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			assertThat(received.getPayload(), Matchers.instanceOf(String.class));


			CollectionLikeType valueType = TypeFactory.defaultInstance()
					.constructCollectionLikeType(List.class, Map.class);

			List<Map<?, ?>> payload = this.objectMapper.readValue((String) received.getPayload(), valueType);

			assertEquals(2, payload.size());
			assertEquals(1, payload.get(0).get("ID"));
			assertEquals(2, payload.get(1).get("ID"));
			received = messageCollector.forChannel(source.output()).poll(10, TimeUnit.SECONDS);
			assertNotNull(received);
			payload = this.objectMapper.readValue((String) received.getPayload(), valueType);
			assertEquals(1, payload.size());
			assertEquals(3, payload.get(0).get("ID"));
		}

	}

	@SpringBootApplication
	public static class JdbcSourceApplication {

	}

}
