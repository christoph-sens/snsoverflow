package com.christophsens.snsoverflow

import aws.sdk.kotlin.services.sns.SnsClient
import aws.sdk.kotlin.services.sns.model.MessageAttributeValue
import aws.sdk.kotlin.services.sns.model.PublishBatchRequest
import aws.sdk.kotlin.services.sns.model.PublishBatchRequestEntry
import aws.sdk.kotlin.services.sns.model.PublishBatchResponse
import aws.sdk.kotlin.services.sns.model.PublishRequest
import aws.sdk.kotlin.services.sns.model.PublishResponse
import com.christophsens.s3overflow.PayloadStore
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import kotlin.test.Test

private const val TOPIC_ARN = "arn:aws:sns:eu-central-1:123456789012:my-topic"

class SnsExtendedClientTest {
    private val snsClient = mockk<SnsClient>()
    private val payloadStore = mockk<PayloadStore>()
    private val config = SnsExtendedClientConfig(payloadStore, payloadSizeThreshold = 20)
    private val client = SnsExtendedClient(snsClient, config)

    @Test
    fun `publishes a small message unchanged`() =
        runTest {
            coEvery { snsClient.publish(any<PublishRequest>()) } returns PublishResponse {}

            client.publish(PublishRequest { topicArn = TOPIC_ARN; message = "small" })

            coVerify {
                snsClient.publish(
                    withArg<PublishRequest> {
                        assertThat(it.message).isEqualTo("small")
                        assertThat(it.messageAttributes.orEmpty()).isEmpty()
                    },
                )
            }
        }

    @Test
    fun `passes through a request with a null or empty message unchanged`() =
        runTest {
            val request = PublishRequest { topicArn = TOPIC_ARN }
            coEvery { snsClient.publish(request) } returns PublishResponse {}

            client.publish(request)

            coVerify { snsClient.publish(request) }
        }

    @Test
    fun `offloads a message body larger than the threshold`() =
        runTest {
            coEvery { payloadStore.storeOriginalPayload(any(), any()) } returns "pointer-json"
            coEvery { snsClient.publish(any<PublishRequest>()) } returns PublishResponse {}

            client.publish(PublishRequest { topicArn = TOPIC_ARN; message = "this message is definitely too large" })

            coVerify {
                snsClient.publish(
                    withArg<PublishRequest> {
                        assertThat(it.message).isEqualTo("pointer-json")
                        assertThat(it.messageAttributes.orEmpty()).containsKey(RESERVED_ATTRIBUTE_NAME)
                    },
                )
            }
        }

    @Test
    fun `offloads every message when alwaysThroughS3 is enabled`() =
        runTest {
            val alwaysS3Client = SnsExtendedClient(snsClient, SnsExtendedClientConfig(payloadStore, alwaysThroughS3 = true))
            coEvery { payloadStore.storeOriginalPayload(any(), any()) } returns "pointer-json"
            coEvery { snsClient.publish(any<PublishRequest>()) } returns PublishResponse {}

            alwaysS3Client.publish(PublishRequest { topicArn = TOPIC_ARN; message = "tiny" })

            coVerify {
                snsClient.publish(withArg<PublishRequest> { assertThat(it.message).isEqualTo("pointer-json") })
            }
        }

    @Test
    fun `stores the offloaded payload under the configured s3 key prefix`() =
        runTest {
            val prefixedClient =
                SnsExtendedClient(snsClient, SnsExtendedClientConfig(payloadStore, payloadSizeThreshold = 20, s3KeyPrefix = "orders/"))
            coEvery { payloadStore.storeOriginalPayload(any(), any()) } returns "pointer-json"
            coEvery { snsClient.publish(any<PublishRequest>()) } returns PublishResponse {}

            prefixedClient.publish(PublishRequest { topicArn = TOPIC_ARN; message = "this message is definitely too large" })

            coVerify {
                payloadStore.storeOriginalPayload(any(), withArg<String> { assertThat(it).startsWith("orders/") })
            }
        }

    @Test
    fun `rejects a message using the multi-protocol json structure`() {
        val request =
            PublishRequest {
                topicArn = TOPIC_ARN
                message = "this message is definitely too large"
                messageStructure = "json"
            }

        assertThatThrownBy { runTest { client.publish(request) } }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects a message that already uses the reserved attribute name`() {
        val request =
            PublishRequest {
                topicArn = TOPIC_ARN
                message = "this message is definitely too large"
                messageAttributes = mapOf(RESERVED_ATTRIBUTE_NAME to MessageAttributeValue { dataType = "Number"; stringValue = "1" })
            }

        assertThatThrownBy { runTest { client.publish(request) } }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `rejects a message with too many attributes`() {
        val attributes =
            (1..MAX_ALLOWED_ATTRIBUTES + 1).associate { i ->
                "attr-$i" to MessageAttributeValue { dataType = "String"; stringValue = "v" }
            }
        val request =
            PublishRequest {
                topicArn = TOPIC_ARN
                message = "this message is definitely too large"
                messageAttributes = attributes
            }

        assertThatThrownBy { runTest { client.publish(request) } }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `offloads only the batch entries larger than the threshold`() =
        runTest {
            coEvery { payloadStore.storeOriginalPayload(any(), any()) } returns "pointer-json"
            coEvery { snsClient.publishBatch(any<PublishBatchRequest>()) } returns
                PublishBatchResponse { failed = emptyList(); successful = emptyList() }

            val request =
                PublishBatchRequest {
                    topicArn = TOPIC_ARN
                    publishBatchRequestEntries =
                        listOf(
                            PublishBatchRequestEntry { id = "small"; message = "tiny" },
                            PublishBatchRequestEntry { id = "large"; message = "this message is definitely too large" },
                        )
                }
            client.publishBatch(request)

            coVerify {
                snsClient.publishBatch(
                    withArg<PublishBatchRequest> { batch ->
                        val small = batch.publishBatchRequestEntries.orEmpty().single { it.id == "small" }
                        val large = batch.publishBatchRequestEntries.orEmpty().single { it.id == "large" }
                        assertThat(small.message).isEqualTo("tiny")
                        assertThat(large.message).isEqualTo("pointer-json")
                    },
                )
            }
        }
}
