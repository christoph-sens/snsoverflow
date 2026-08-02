/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2026 Christoph Sens
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "LICENSE" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * This file is a Kotlin port of the message-offload control flow from
 * amazon-sns-java-extended-client-lib's AmazonSNSExtendedClient/-ClientBase/-ClientUtil
 * (https://github.com/awslabs/amazon-sns-java-extended-client-lib), adapted for
 * aws-sdk-kotlin, coroutines, and s3overflow. See NOTICE for details.
 */

package com.christophsens.snsoverflow

import aws.sdk.kotlin.services.sns.SnsClient
import aws.sdk.kotlin.services.sns.model.MessageAttributeValue
import aws.sdk.kotlin.services.sns.model.PublishBatchRequest
import aws.sdk.kotlin.services.sns.model.PublishBatchRequestEntry
import aws.sdk.kotlin.services.sns.model.PublishBatchResponse
import aws.sdk.kotlin.services.sns.model.PublishRequest
import aws.sdk.kotlin.services.sns.model.PublishResponse
import com.christophsens.s3overflow.payloadSizeInBytes
import com.christophsens.s3overflow.storeOriginalPayload
import java.util.UUID

/**
 * Wraps an aws-sdk-kotlin [SnsClient] and transparently offloads message bodies that exceed
 * [SnsExtendedClientConfig.payloadSizeThreshold] to the configured payload store, publishing only
 * a pointer through SNS. Unlike SQS, SNS is fire-and-forget, so there is no receive/delete side to
 * this client: subscribers resolve the pointer themselves (e.g. a queue subscribed with raw
 * message delivery, read via sqsoverflow's `SqsExtendedClient`, which recognizes the same
 * reserved attribute name).
 */
class SnsExtendedClient(
    private val snsClient: SnsClient,
    private val clientConfig: SnsExtendedClientConfig,
) : SnsClient by snsClient {
    override suspend fun publish(input: PublishRequest): PublishResponse {
        val body = input.message
        if (body.isNullOrEmpty()) return snsClient.publish(input)

        require(input.messageStructure != MULTIPLE_PROTOCOL_MESSAGE_STRUCTURE) {
            "SnsExtendedClient does not support sending JSON messages."
        }

        if (!clientConfig.alwaysThroughS3 && !isLarge(body, input.messageAttributes)) return snsClient.publish(input)

        checkMessageAttributes(input.messageAttributes)

        val pointer = storeOriginalPayload(body)
        val request =
            input.copy {
                messageAttributes = withSizeAttribute(input.messageAttributes, payloadSizeInBytes(body))
                message = pointer
            }
        return snsClient.publish(request)
    }

    override suspend fun publishBatch(input: PublishBatchRequest): PublishBatchResponse {
        val entries = input.publishBatchRequestEntries.orEmpty().map { entry -> offloadIfNeeded(entry) }
        return snsClient.publishBatch(input.copy { publishBatchRequestEntries = entries })
    }

    private suspend fun offloadIfNeeded(entry: PublishBatchRequestEntry): PublishBatchRequestEntry {
        val body = entry.message
        if (body.isNullOrEmpty()) return entry

        require(entry.messageStructure != MULTIPLE_PROTOCOL_MESSAGE_STRUCTURE) {
            "SnsExtendedClient does not support sending JSON messages."
        }

        if (!clientConfig.alwaysThroughS3 && !isLarge(body, entry.messageAttributes)) return entry

        checkMessageAttributes(entry.messageAttributes)

        val pointer = storeOriginalPayload(body)
        return entry.copy {
            messageAttributes = withSizeAttribute(entry.messageAttributes, payloadSizeInBytes(body))
            message = pointer
        }
    }

    private suspend fun storeOriginalPayload(body: String): String {
        val prefix = clientConfig.s3KeyPrefix
        return if (prefix.isEmpty()) {
            clientConfig.payloadStore.storeOriginalPayload(body)
        } else {
            clientConfig.payloadStore.storeOriginalPayload(body, prefix + UUID.randomUUID())
        }
    }

    private fun checkMessageAttributes(attributes: Map<String, MessageAttributeValue>?) {
        val attrs = attributes.orEmpty()
        val size = attributesSizeInBytes(attrs)
        require(size <= clientConfig.payloadSizeThreshold) {
            "Total size of message attributes is $size bytes which is larger than the threshold of " +
                "${clientConfig.payloadSizeThreshold} bytes. Consider including the payload in the message body instead " +
                "of message attributes."
        }
        require(attrs.size <= MAX_ALLOWED_ATTRIBUTES) {
            "Number of message attributes [${attrs.size}] exceeds the maximum allowed for large-payload " +
                "messages [$MAX_ALLOWED_ATTRIBUTES]."
        }
        require(RESERVED_ATTRIBUTE_NAME !in attrs) {
            "Message attribute name $RESERVED_ATTRIBUTE_NAME is reserved for use by SnsExtendedClient."
        }
    }

    private fun isLarge(body: String, attributes: Map<String, MessageAttributeValue>?): Boolean =
        attributesSizeInBytes(attributes.orEmpty()) + payloadSizeInBytes(body) > clientConfig.payloadSizeThreshold

    private fun attributesSizeInBytes(attributes: Map<String, MessageAttributeValue>): Long =
        attributes.entries.sumOf { (key, value) ->
            payloadSizeInBytes(key) +
                payloadSizeInBytes(value.dataType) +
                (value.stringValue?.let(::payloadSizeInBytes) ?: 0) +
                (value.binaryValue?.size?.toLong() ?: 0)
        }

    private fun withSizeAttribute(
        attributes: Map<String, MessageAttributeValue>?,
        payloadSize: Long,
    ): Map<String, MessageAttributeValue> =
        attributes.orEmpty() +
            (
                RESERVED_ATTRIBUTE_NAME to
                    MessageAttributeValue {
                        dataType = "Number"
                        stringValue = payloadSize.toString()
                    }
            )
}
