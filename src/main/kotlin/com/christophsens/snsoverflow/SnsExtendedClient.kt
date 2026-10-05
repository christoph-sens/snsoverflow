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
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import java.util.UUID

/**
 * Returns an [SnsClient] that wraps [snsClient] and transparently offloads message bodies that exceed
 * [SnsExtendedClientConfig.payloadSizeThreshold] to the configured payload store, publishing only
 * a pointer through SNS. Unlike SQS, SNS is fire-and-forget, so there is no receive/delete side to
 * this client: subscribers resolve the pointer themselves (e.g. a queue subscribed with raw
 * message delivery, read via sqsoverflow's `SqsExtendedClient`, which recognizes the same
 * reserved attribute name).
 *
 * Payloads of a batch are uploaded concurrently. Since SNS checks the sum of all messages in a batch
 * against the topic's `MaximumMessageSize`, [SnsExtendedClientConfig.payloadSizeThreshold] doubles as
 * the batch limit: the largest entries are offloaded until the batch fits.
 *
 * The returned client is a dynamic proxy for the [SnsClient] interface of the aws-sdk-kotlin version on
 * the runtime classpath: `publish` and `publishBatch` go through the offloading logic, every other
 * operation goes straight to [snsClient]. Operations added by a newer aws-sdk-kotlin therefore work
 * without a new snsoverflow release.
 */
fun SnsExtendedClient(
    snsClient: SnsClient,
    clientConfig: SnsExtendedClientConfig,
): SnsClient = offloadingProxy(OffloadingSnsClient(snsClient, clientConfig), snsClient, OFFLOADED_OPERATIONS)

/** Operations that [OffloadingSnsClient] overrides; all others are forwarded to the wrapped client by the proxy. */
internal val OFFLOADED_OPERATIONS = setOf("publish", "publishBatch")

/**
 * The offloading logic behind [SnsExtendedClient]. Only reached through the proxy, and only for
 * [OFFLOADED_OPERATIONS]: its delegated members are compiled against one aws-sdk-kotlin version.
 */
internal class OffloadingSnsClient(
    private val snsClient: SnsClient,
    private val clientConfig: SnsExtendedClientConfig,
) : SnsClient by snsClient {
    override suspend fun publish(input: PublishRequest): PublishResponse {
        checkReservedAttributes(input.messageAttributes)
        val body = input.message
        if (body.isNullOrEmpty()) return snsClient.publish(input)
        if (!clientConfig.alwaysThroughS3 && !isLarge(body, input.messageAttributes)) return snsClient.publish(input)

        checkOffloadable(input.messageStructure, input.messageAttributes)
        val pointer = storeOriginalPayload(body)
        val request =
            input.copy {
                messageAttributes = withSizeAttribute(input.messageAttributes, payloadSizeInBytes(body))
                message = pointer
            }
        return snsClient.publish(request)
    }

    override suspend fun publishBatch(input: PublishBatchRequest): PublishBatchResponse {
        val entries = input.publishBatchRequestEntries.orEmpty()
        entries.forEach { entry -> checkReservedAttributes(entry.messageAttributes) }

        val toOffload = entriesToOffload(entries)
        val prepared =
            coroutineScope {
                entries.mapIndexed { index, entry -> async { if (index in toOffload) offload(entry) else entry } }.awaitAll()
            }
        return snsClient.publishBatch(input.copy { publishBatchRequestEntries = prepared })
    }

    /**
     * Indices of the batch entries to offload: every entry above the threshold, plus the largest remaining
     * entries while the sum of all messages would exceed the threshold.
     */
    private fun entriesToOffload(entries: List<PublishBatchRequestEntry>): Set<Int> {
        val sizes = entries.map { messageSizeInBytes(it.message.orEmpty(), it.messageAttributes) }
        val offloadedSizes = entries.map { attributesSizeInBytes(it.messageAttributes.orEmpty()) + OFFLOADED_BODY_ALLOWANCE_BYTES }
        val offloadable = entries.indices.filter { !entries[it].message.isNullOrEmpty() }
        val toOffload = offloadable.filterTo(mutableSetOf()) { clientConfig.alwaysThroughS3 || sizes[it] > clientConfig.payloadSizeThreshold }

        var total = entries.indices.sumOf { if (it in toOffload) offloadedSizes[it] else sizes[it] }
        val candidates = (offloadable - toOffload).filter { sizes[it] > offloadedSizes[it] }.sortedByDescending { sizes[it] }.iterator()
        while (total > clientConfig.payloadSizeThreshold && candidates.hasNext()) {
            val index = candidates.next()
            toOffload += index
            total += offloadedSizes[index] - sizes[index]
        }
        toOffload.forEach { checkOffloadable(entries[it].messageStructure, entries[it].messageAttributes) }
        return toOffload
    }

    private suspend fun offload(entry: PublishBatchRequestEntry): PublishBatchRequestEntry {
        val body = entry.message.orEmpty()
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

    private fun checkReservedAttributes(attributes: Map<String, MessageAttributeValue>?) {
        val attrs = attributes.orEmpty()
        RESERVED_ATTRIBUTE_NAMES.forEach { name ->
            require(name !in attrs) { "Message attribute name $name is reserved for use by SnsExtendedClient." }
        }
    }

    private fun checkOffloadable(messageStructure: String?, attributes: Map<String, MessageAttributeValue>?) {
        require(messageStructure != MULTIPLE_PROTOCOL_MESSAGE_STRUCTURE) {
            "SnsExtendedClient cannot offload messages with the multi-protocol JSON message structure."
        }
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
    }

    private fun isLarge(body: String, attributes: Map<String, MessageAttributeValue>?): Boolean =
        messageSizeInBytes(body, attributes) > clientConfig.payloadSizeThreshold

    private fun messageSizeInBytes(body: String, attributes: Map<String, MessageAttributeValue>?): Long =
        attributesSizeInBytes(attributes.orEmpty()) + payloadSizeInBytes(body)

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
