using System;
using Xunit;
using Confluent.Kafka;
using Moq;
using System.Threading.Tasks;

namespace Api.Tests
{
    public class ProducerWrapperTests
    {
        [Fact]
        public async Task WriteMessage_ValidMessage_ShouldProduceSuccessfully()
        {
            // Arrange
            var mockConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };
            var producerWrapper = new ProducerWrapper(mockConfig, "test-topic");

            // Act
            await producerWrapper.writeMessage("test message");

            // Assert
            // Verify no exception was thrown
        }

        [Fact]
        public async Task WriteMessage_NullMessage_ShouldHandleGracefully()
        {
            // Arrange
            var mockConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };
            var producerWrapper = new ProducerWrapper(mockConfig, "test-topic");

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(() => producerWrapper.writeMessage(null));
        }

        [Fact]
        public void Constructor_NullConfig_ShouldThrowArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new ProducerWrapper(null, "test-topic"));
        }

        [Fact]
        public void Constructor_EmptyTopicName_ShouldThrowArgumentException()
        {
            // Arrange
            var mockConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };

            // Act & Assert
            Assert.Throws<ArgumentException>(() => new ProducerWrapper(mockConfig, string.Empty));
        }

        [Fact]
        public async Task WriteMessage_LongMessage_ShouldProduceSuccessfully()
        {
            // Arrange
            var mockConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };
            var producerWrapper = new ProducerWrapper(mockConfig, "test-topic");
            var longMessage = new string('x', 1000);

            // Act
            await producerWrapper.writeMessage(longMessage);

            // Assert
            // Verify no exception was thrown
        }
    }
}