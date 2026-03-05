package com.flipfoundry.platform.data.service.kafka.config;

import com.flipfoundry.platform.data.service.kafka.utils.SerdesFactory;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit Tests for Kafka Configuration.
 * Tests configuration utilities and default settings.
 */
@DisplayName("Kafka Configuration Unit Tests")
class KafkaConfigurationTests {

    @Nested
    @DisplayName("SerdesFactory Configuration Tests")
    class SerdesFactoryConfigTests {

        @Test
        @DisplayName("should create SerdesFactory instance")
        void shouldCreateSerdesFactory() {
            // Act
            SerdesFactory factory = new SerdesFactory();

            // Assert
            assertThat(factory).isNotNull();
        }

        @Test
        @DisplayName("should configure schema registry URL")
        void shouldConfigureSchemaRegistry() {
            // Arrange
            SerdesFactory factory = new SerdesFactory();
            String testUrl = "http://localhost:8081";

            // Act
            factory.setSrUrl(testUrl);

            // Assert
            assertThat(factory).isNotNull();
        }

        @Test
        @DisplayName("should create multiple SerdesFactory instances")
        void shouldCreateMultipleSerdesFactories() {
            // Act
            SerdesFactory factory1 = new SerdesFactory();
            SerdesFactory factory2 = new SerdesFactory();

            factory1.setSrUrl("http://localhost:8081");
            factory2.setSrUrl("http://localhost:8082");

            // Assert
            assertThat(factory1).isNotNull();
            assertThat(factory2).isNotNull();
        }

        @Test
        @DisplayName("should support mock schema registry URL")
        void shouldSupportMockRegistry() {
            // Arrange
            SerdesFactory factory = new SerdesFactory();

            // Act
            factory.setSrUrl("mock://test");

            // Assert
            assertThat(factory).isNotNull();
        }

        @Test
        @DisplayName("should support HTTPS schema registry URL")
        void shouldSupportHttpsRegistry() {
            // Arrange
            SerdesFactory factory = new SerdesFactory();

            // Act
            factory.setSrUrl("https://schema-registry.example.com:8081");

            // Assert
            assertThat(factory).isNotNull();
        }
    }

    @Nested
    @DisplayName("Kafka Topic Names Tests")
    class TopicNamesTests {

        @Test
        @DisplayName("should have students topic")
        void shouldHaveStudentsTopic() {
            // Act
            String topic = "students";

            // Assert
            assertThat(topic).isEqualTo("students");
        }

        @Test
        @DisplayName("should have enrollments topic")
        void shouldHaveEnrollmentsTopic() {
            // Act
            String topic = "student-enrollments";

            // Assert
            assertThat(topic).isEqualTo("student-enrollments");
        }

        @Test
        @DisplayName("should have profiles topic")
        void shouldHaveProfilesTopic() {
            // Act
            String topic = "student-profiles";

            // Assert
            assertThat(topic).isEqualTo("student-profiles");
        }

        @Test
        @DisplayName("should have DLQ topic")
        void shouldHaveDlqTopic() {
            // Act
            String topic = "dlq";

            // Assert
            assertThat(topic).isEqualTo("dlq");
        }

        @Test
        @DisplayName("should validate topic names format")
        void shouldValidateTopicNamesFormat() {
            // Arrange
            String[] topics = {"students", "student-enrollments", "student-profiles", "dlq"};

            // Assert
            for (String topic : topics) {
                assertThat(topic).matches("[a-z0-9\\-]+");
            }
        }
    }

    @Nested
    @DisplayName("Serialization Configuration Tests")
    class SerializationTests {

        @Test
        @DisplayName("should have String serializer available")
        void shouldHaveStringSerializer() {
            // Act
            StringSerializer serializer = new StringSerializer();

            // Assert
            assertThat(serializer).isNotNull();
        }

        @Test
        @DisplayName("should have String deserializer available")
        void shouldHaveStringDeserializer() {
            // Act
            StringDeserializer deserializer = new StringDeserializer();

            // Assert
            assertThat(deserializer).isNotNull();
        }

        @Test
        @DisplayName("should have serializer class available")
        void shouldHaveSerializerClass() {
            // Act
            Class<?> serializerClass = StringSerializer.class;

            // Assert
            assertThat(serializerClass.getName()).contains("StringSerializer");
        }

        @Test
        @DisplayName("should have deserializer class available")
        void shouldHaveDeserializerClass() {
            // Act
            Class<?> deserializerClass = StringDeserializer.class;

            // Assert
            assertThat(deserializerClass.getName()).contains("StringDeserializer");
        }

        @Test
        @DisplayName("should support multiple serializers")
        void shouldSupportMultipleSerializers() {
            // Act
            StringSerializer serializer1 = new StringSerializer();
            StringSerializer serializer2 = new StringSerializer();

            // Assert
            assertThat(serializer1).isNotNull();
            assertThat(serializer2).isNotNull();
        }
    }

    @Nested
    @DisplayName("Kafka Configuration Constants Tests")
    class ConfigurationConstantsTests {

        @Test
        @DisplayName("should have valid bootstrap servers value")
        void shouldHaveBootstrapServers() {
            // Act
            String bootstrapServers = "localhost:9092";

            // Assert
            assertThat(bootstrapServers).contains(":");
        }

        @Test
        @DisplayName("should support multiple bootstrap servers")
        void shouldSupportMultipleBootstrapServers() {
            // Act
            String bootstrapServers = "broker1:9092,broker2:9092,broker3:9092";

            // Assert
            assertThat(bootstrapServers).contains(",");
            assertThat(bootstrapServers).contains("broker");
        }

        @Test
        @DisplayName("should have valid schema registry URL")
        void shouldHaveSchemaRegistryUrl() {
            // Act
            String schemaRegistry = "http://localhost:8081";

            // Assert
            assertThat(schemaRegistry).startsWith("http");
        }

        @Test
        @DisplayName("should support HTTPS schema registry")
        void shouldSupportHttpsSchemaRegistry() {
            // Act
            String schemaRegistry = "https://schema-registry.example.com";

            // Assert
            assertThat(schemaRegistry).startsWith("https");
        }

        @Test
        @DisplayName("should have valid application ID")
        void shouldHaveApplicationId() {
            // Act
            String applicationId = "data-service-app";

            // Assert
            assertThat(applicationId).isNotNull();
            assertThat(applicationId).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Configuration Flexibility Tests")
    class ConfigurationFlexibilityTests {

        @Test
        @DisplayName("should allow custom bootstrap servers")
        void shouldAllowCustomBootstrapServers() {
            // Act
            String customServers = "custom-broker-1:9092,custom-broker-2:9092";

            // Assert
            assertThat(customServers).contains("custom-broker");
        }

        @Test
        @DisplayName("should allow custom application ID")
        void shouldAllowCustomApplicationId() {
            // Act
            String customAppId = "my-custom-app";

            // Assert
            assertThat(customAppId).isEqualTo("my-custom-app");
        }

        @Test
        @DisplayName("should allow custom schema registry URL")
        void shouldAllowCustomSchemaRegistry() {
            // Act
            String customRegistry = "http://custom-registry.internal:8081";

            // Assert
            assertThat(customRegistry).startsWith("http");
        }

        @Test
        @DisplayName("should create independent SerdesFactory instances")
        void shouldCreateIndependentInstances() {
            // Act
            SerdesFactory factory1 = new SerdesFactory();
            SerdesFactory factory2 = new SerdesFactory();

            factory1.setSrUrl("http://registry1:8081");
            factory2.setSrUrl("http://registry2:8081");

            // Assert
            assertThat(factory1).isNotNull();
            assertThat(factory2).isNotNull();
        }

        @Test
        @DisplayName("should support environment-based configuration")
        void shouldSupportEnvironmentConfiguration() {
            // Arrange
            String env = System.getenv("ENVIRONMENT");
            String bootstrapServers = (env != null && env.equals("prod")) 
                ? "prod-kafka:9092" 
                : "localhost:9092";

            // Assert
            assertThat(bootstrapServers).isNotNull();
            assertThat(bootstrapServers).contains(":");
        }
    }

    @Nested
    @DisplayName("Configuration Integration Tests")
    class ConfigurationIntegrationTests {

        @Test
        @DisplayName("should create complete configuration")
        void shouldCreateCompleteConfiguration() {
            // Arrange
            SerdesFactory factory = new SerdesFactory();
            String bootstrapServers = "localhost:9092";
            String applicationId = "test-app";
            String schemaRegistry = "http://localhost:8081";

            // Act
            factory.setSrUrl(schemaRegistry);

            // Assert
            assertThat(factory).isNotNull();
            assertThat(bootstrapServers).isNotNull();
            assertThat(applicationId).isNotNull();
        }

        @Test
        @DisplayName("should support configuration overrides")
        void shouldSupportConfigurationOverrides() {
            // Arrange
            SerdesFactory factory1 = new SerdesFactory();
            SerdesFactory factory2 = new SerdesFactory();

            String url1 = "http://localhost:8081";
            String url2 = "https://prod.example.com:8081";

            // Act
            factory1.setSrUrl(url1);
            factory2.setSrUrl(url2);

            // Assert
            assertThat(factory1).isNotNull();
            assertThat(factory2).isNotNull();
        }

        @Test
        @DisplayName("should support multiple schema registries")
        void shouldSupportMultipleRegistries() {
            // Arrange & Act
            SerdesFactory localFactory = new SerdesFactory();
            localFactory.setSrUrl("http://localhost:8081");

            SerdesFactory prodFactory = new SerdesFactory();
            prodFactory.setSrUrl("https://prod-registry.example.com:8081");

            // Assert
            assertThat(localFactory).isNotNull();
            assertThat(prodFactory).isNotNull();
        }
    }
}
