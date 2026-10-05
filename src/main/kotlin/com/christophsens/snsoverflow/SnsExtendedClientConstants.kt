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
 * This file is a Kotlin port of the reserved-attribute and S3-key-prefix validation logic
 * from amazon-sns-java-extended-client-lib's SNSExtendedClientConstants/AmazonSNSExtendedClientUtil
 * and amazon-sqs-java-extended-client-lib's SQSExtendedClientConstants
 * (https://github.com/awslabs/amazon-sns-java-extended-client-lib). RESERVED_ATTRIBUTE_NAME
 * is kept byte-identical to the original's reserved attribute name (borrowed from the SQS
 * extended client), since the original documents it as shared with SQSExtendedClient
 * implementations for SNS-to-SQS fan-out interop. See NOTICE for details.
 */

package com.christophsens.snsoverflow

/** Message attribute name used to flag a message whose body was offloaded to S3. */
internal const val RESERVED_ATTRIBUTE_NAME = "ExtendedPayloadSize"

/**
 * Former reserved attribute name that amazon-sqs-java-extended-client-lib (and sqsoverflow) still
 * recognize on receive. Rejected in user attributes so subscribers don't mistake a message for an
 * offloaded one; never written.
 */
internal const val LEGACY_RESERVED_ATTRIBUTE_NAME = "SQSLargePayloadSize"

internal val RESERVED_ATTRIBUTE_NAMES = listOf(RESERVED_ATTRIBUTE_NAME, LEGACY_RESERVED_ATTRIBUTE_NAME)

/** SNS allows at most 10 message attributes; one slot is reserved for [RESERVED_ATTRIBUTE_NAME]. */
internal const val MAX_ALLOWED_ATTRIBUTES = 9

/**
 * Upper bound for the size of an offloaded message's body and size attribute: the pointer JSON with a
 * bucket name of up to 63 and a key of up to 1024 characters, plus the `ExtendedPayloadSize` attribute.
 */
internal const val OFFLOADED_BODY_ALLOWANCE_BYTES = 1280L

/** SNS's multi-protocol JSON message structure, which SnsExtendedClient does not support offloading for. */
internal const val MULTIPLE_PROTOCOL_MESSAGE_STRUCTURE = "json"

private const val MAX_S3_KEY_LENGTH = 1024
private const val UUID_LENGTH = 36
internal const val MAX_S3_KEY_PREFIX_LENGTH = MAX_S3_KEY_LENGTH - UUID_LENGTH

private val INVALID_S3_KEY_PREFIX_CHARACTERS = Regex("[^a-zA-Z0-9./_-]")

/** Validates and trims an S3 key prefix, throwing [IllegalArgumentException] if it isn't usable. */
internal fun validateS3KeyPrefix(s3KeyPrefix: String): String {
    val trimmed = s3KeyPrefix.trim()
    require(trimmed.length <= MAX_S3_KEY_PREFIX_LENGTH) {
        "The S3 key prefix length must not be greater than $MAX_S3_KEY_PREFIX_LENGTH"
    }
    require(!trimmed.startsWith(".") && !trimmed.startsWith("/")) {
        "The S3 key prefix must not start with '.' or '/'"
    }
    require(!trimmed.contains("..")) { "The S3 key prefix must not contain the string '..'" }
    require(!INVALID_S3_KEY_PREFIX_CHARACTERS.containsMatchIn(trimmed)) {
        "The S3 key prefix contains invalid characters. Allowed: letters, digits, '/', '_', '-', and '.'"
    }
    return trimmed
}
