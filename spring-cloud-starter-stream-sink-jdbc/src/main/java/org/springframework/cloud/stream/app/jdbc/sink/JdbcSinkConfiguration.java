/*
 * Copyright 2015-2017 the original author or authors.
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

package org.springframework.cloud.stream.app.jdbc.sink;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.app.jdbc.DefaultInitializationScriptResource;
import org.springframework.cloud.stream.app.jdbc.ShorthandMapConverter;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ResourceLoader;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.SpelParseException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.jdbc.JdbcMessageHandler;
import org.springframework.integration.json.JsonPropertyAccessor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.messaging.Message;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

/**
 * A module that writes its incoming payload to an RDBMS using JDBC.
 *
 * @author Eric Bottard
 * @author Thomas Risberg
 */
@EnableBinding(Sink.class)
@EnableConfigurationProperties(JdbcSinkProperties.class)
public class JdbcSinkConfiguration {

	private static class SqlParameterSourceFactory implements org.springframework.integration.jdbc.SqlParameterSourceFactory{

		private final MultiValueMap<String, Expression> columnExpressions;
		private final EvaluationContext context;

		SqlParameterSourceFactory(MultiValueMap<String, Expression> columnExpressions, EvaluationContext context) {
			this.columnExpressions = columnExpressions;
			this.context = context;
		}

		@Override
		public SqlParameterSource createParameterSource(Object o) {
			if (!(o instanceof Message)) {
				throw new IllegalArgumentException("Unable to handle type " + o.getClass().getName());
			}
			Message<?> message = (Message<?>) o;
			MapSqlParameterSource parameterSource = new MapSqlParameterSource();
			for (String key : columnExpressions.keySet()) {
				List<Expression> spels = columnExpressions.get(key);
				Object value = NOT_SET;
				EvaluationException lastException = null;
				for (Expression spel : spels) {
					try {
						value = spel.getValue(context, message);
						break;
					} catch (EvaluationException e) {
						lastException = e;
					}
				}
				if (value == NOT_SET) {
					if (lastException != null) {
						logger.info("Could not find value for column '" + key + "': " + lastException.getMessage());
					}
					parameterSource.addValue(key, null);
				} else {
					if (value instanceof JsonPropertyAccessor.ToStringFriendlyJsonNode) {
						// Need to do some reflection until we have a getter for the Node
						DirectFieldAccessor dfa = new DirectFieldAccessor(value);
						JsonNode node = (JsonNode) dfa.getPropertyValue("node");
						Object valueToUse;
						if (node == null || node.isNull()) {
							valueToUse = null;
						} else if (node.isNumber()) {
							valueToUse = node.numberValue();
						} else if (node.isBoolean()) {
							valueToUse = node.booleanValue();
						} else {
							valueToUse = node.textValue();
						}
						parameterSource.addValue(key, valueToUse);
					} else {
						parameterSource.addValue(key, value);
					}
				}
			}
			return parameterSource;
		}
	}

	private static final Log logger = LogFactory.getLog(JdbcSinkConfiguration.class);

	private static final Object NOT_SET = new Object();

	private SpelExpressionParser spelExpressionParser = new SpelExpressionParser();

	@Autowired
	private BeanFactory beanFactory;

	@Autowired
	private JdbcSinkProperties properties;

	protected EvaluationContext evaluationContext;


	@Bean
	@ServiceActivator(autoStartup = "true", inputChannel = Sink.INPUT)
	public JdbcMessageHandler jdbcMessageHandler(DataSource dataSource) {
		final MultiValueMap<String, Expression> columnExpressionVariations = new LinkedMultiValueMap<>();
		for (Map.Entry<String, String> entry : properties.getColumns().entrySet()) {
			String value = entry.getValue();
			columnExpressionVariations.add(entry.getKey(), spelExpressionParser.parseExpression(value));
			if (!value.startsWith("payload")) {
				String qualified = "payload." + value;
				try {
					columnExpressionVariations.add(entry.getKey(), spelExpressionParser.parseExpression(qualified));
				}
				catch (SpelParseException e) {
					logger.info("failed to parse qualified fallback expression " + qualified +
						"; be sure your expression uses the 'payload.' prefix where necessary");
				}
			}
		}
		JdbcMessageHandler jdbcMessageHandler = new JdbcMessageHandler(dataSource,
				generateSql(properties.getTableName(), columnExpressionVariations.keySet()));
		SqlParameterSourceFactory parameterSourceFactory = new SqlParameterSourceFactory(
				columnExpressionVariations, evaluationContext);
		jdbcMessageHandler.setSqlParameterSourceFactory(parameterSourceFactory);
		return jdbcMessageHandler;
	}

	@ConditionalOnProperty("jdbc.initialize")
	@Bean
	public DataSourceInitializer nonBootDataSourceInitializer(DataSource dataSource, ResourceLoader resourceLoader) {
		DataSourceInitializer dataSourceInitializer = new DataSourceInitializer();
		dataSourceInitializer.setDataSource(dataSource);
		ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();
		databasePopulator.setIgnoreFailedDrops(true);
		dataSourceInitializer.setDatabasePopulator(databasePopulator);
		if ("true".equals(properties.getInitialize())) {
			databasePopulator.addScript(new DefaultInitializationScriptResource(properties.getTableName(),
					properties.getColumns().keySet()));
		} else {
			databasePopulator.addScript(resourceLoader.getResource(properties.getInitialize()));
		}
		return dataSourceInitializer;
	}

	/*
	 * This is needed to prevent a circular dependency issue with the creation of the converter.
	 */
	public static class Nested {

		@Bean
		@ConfigurationPropertiesBinding
		public ShorthandMapConverter shorthandMapConverter() {
			return new ShorthandMapConverter();
		}

	}

	@PostConstruct
	public void afterPropertiesSet() {
		this.evaluationContext = ExpressionUtils.createStandardEvaluationContext(beanFactory);
	}

	private String generateSql(String tableName, Set<String> columns) {
		StringBuilder builder = new StringBuilder("INSERT INTO ");
		StringBuilder questionMarks = new StringBuilder(") VALUES (");
		builder.append(tableName).append("(");
		int i = 0;

		for (String column : columns) {
			if (i++ > 0) {
				builder.append(", ");
				questionMarks.append(", ");
			}
			builder.append(column);
			questionMarks.append(':' + column);
		}
		builder.append(questionMarks).append(")");
		return builder.toString();
	}

}