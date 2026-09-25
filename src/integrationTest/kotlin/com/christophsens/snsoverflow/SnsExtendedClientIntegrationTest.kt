package com.christophsens.snsoverflow

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.CreateBucketRequest
import aws.sdk.kotlin.services.s3.model.ListObjectsV2Request
import aws.sdk.kotlin.services.sns.SnsClient
import aws.sdk.kotlin.services.sns.model.CreateTopicRequest
import aws.sdk.kotlin.services.sns.model.PublishRequest
import aws.sdk.kotlin.services.sns.model.SubscribeRequest
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.CreateQueueRequest
import aws.sdk.kotlin.services.sqs.model.GetQueueAttributesRequest
import aws.sdk.kotlin.services.sqs.model.QueueAttributeName
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageRequest
import aws.smithy.kotlin.runtime.net.url.Url
import com.christophsens.s3overflow.PayloadS3Pointer
import com.christophsens.s3overflow.S3BackedPayloadStore
import com.christophsens.s3overflow.SNS_DEFAULT_MAX_MESSAGE_SIZE_BYTES
import io.floci.testcontainers.FlociContainer
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.UUID

/**
 * Runs [SnsExtendedClient] against real SNS, SQS, and S3 APIs (via [Floci](https://github.com/floci-io/floci),
 * a free local AWS emulator) to verify the default 256 KiB offload threshold end-to-end: small
 * messages are published to SNS unchanged, larger ones are written to S3 and only a pointer
 * travels through SNS. A queue subscribed to the topic with raw message delivery stands in for a
 * real subscriber, so the delivered message can be inspected directly. Requires Docker.
 *
 * Run with `./gradlew integrationTest`; this is not part of `./gradlew build`/`check`.
 */
@Testcontainers
class SnsExtendedClientIntegrationTest {
    companion object {
        @Container
        @JvmStatic
        val floci: FlociContainer = FlociContainer()
    }

    private val credentialsProvider =
        StaticCredentialsProvider {
            accessKeyId = floci.accessKey
            secretAccessKey = floci.secretKey
        }

    private val snsClient =
        SnsClient {
            endpointUrl = Url.parse(floci.endpoint)
            region = floci.region
            credentialsProvider = this@SnsExtendedClientIntegrationTest.credentialsProvider
        }

    private val sqsClient =
        SqsClient {
            endpointUrl = Url.parse(floci.endpoint)
            region = floci.region
            credentialsProvider = this@SnsExtendedClientIntegrationTest.credentialsProvider
        }

    private val s3Client =
        S3Client {
            endpointUrl = Url.parse(floci.endpoint)
            region = floci.region
            credentialsProvider = this@SnsExtendedClientIntegrationTest.credentialsProvider
            forcePathStyle = true
        }

    private lateinit var testTopicArn: String
    private lateinit var testQueueUrl: String
    private lateinit var testBucketName: String
    private lateinit var extendedClient: SnsExtendedClient

    @BeforeEach
    fun setUp() =
        runTest {
            testTopicArn = snsClient.createTopic(CreateTopicRequest { name = "test-topic-${UUID.randomUUID()}" }).topicArn!!
            testQueueUrl = sqsClient.createQueue(CreateQueueRequest { queueName = "test-queue-${UUID.randomUUID()}" }).queueUrl!!
            val queueArn =
                sqsClient.getQueueAttributes(
                    GetQueueAttributesRequest {
                        queueUrl = testQueueUrl
                        attributeNames = listOf(QueueAttributeName.QueueArn)
                    },
                ).attributes.orEmpty().getValue(QueueAttributeName.QueueArn)

            snsClient.subscribe(
                SubscribeRequest {
                    topicArn = testTopicArn
                    protocol = "sqs"
                    endpoint = queueArn
                    attributes = mapOf("RawMessageDelivery" to "true")
                },
            )

            testBucketName = "test-bucket-${UUID.randomUUID()}"
            s3Client.createBucket(CreateBucketRequest { bucket = testBucketName })

            extendedClient =
                SnsExtendedClient(
                    snsClient,
                    SnsExtendedClientConfig(payloadStore = S3BackedPayloadStore(s3Client, testBucketName)),
                )
        }

    @Test
    fun `messages smaller than 256 KiB are published to SNS unchanged`() =
        runTest {
            val smallBody = "x".repeat(1024)

            extendedClient.publish(PublishRequest { topicArn = testTopicArn; message = smallBody })

            val rawMessage = receiveRawMessage()
            assertThat(rawMessage.body).isEqualTo(smallBody)
            assertThat(rawMessage.messageAttributes.orEmpty()).doesNotContainKey(RESERVED_ATTRIBUTE_NAME)
            assertThat(objectsInBucket()).isEmpty()
        }

    @Test
    fun `messages larger than 256 KiB are stored in S3 and only a pointer travels through SNS`() =
        runTest {
            val largeBody = "x".repeat(SNS_DEFAULT_MAX_MESSAGE_SIZE_BYTES + 1024)

            extendedClient.publish(PublishRequest { topicArn = testTopicArn; message = largeBody })

            val rawMessage = receiveRawMessage()
            assertThat(rawMessage.body).isNotEqualTo(largeBody)
            assertThat(rawMessage.messageAttributes.orEmpty()).containsKey(RESERVED_ATTRIBUTE_NAME)

            val pointer = PayloadS3Pointer.fromJson(requireNotNull(rawMessage.body))
            assertThat(pointer.s3BucketName).isEqualTo(testBucketName)

            val objects = objectsInBucket()
            assertThat(objects).hasSize(1)
            assertThat(objects.single().key).isEqualTo(pointer.s3Key)
        }

    /** Waits for the message SNS delivered (with raw message delivery) to the subscribed queue. */
    private suspend fun receiveRawMessage() =
        sqsClient.receiveMessage(
            ReceiveMessageRequest {
                queueUrl = testQueueUrl
                waitTimeSeconds = 5
                messageAttributeNames = listOf("All")
            },
        ).messages.orEmpty().single()

    private suspend fun objectsInBucket() =
        s3Client.listObjectsV2(ListObjectsV2Request { bucket = testBucketName }).contents.orEmpty()
}
