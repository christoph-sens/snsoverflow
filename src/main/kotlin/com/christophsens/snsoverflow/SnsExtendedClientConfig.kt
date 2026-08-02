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
 * This file is a Kotlin port of configuration logic from
 * amazon-sns-java-extended-client-lib's SNSExtendedClientConfiguration
 * (https://github.com/awslabs/amazon-sns-java-extended-client-lib), adapted for
 * aws-sdk-kotlin and s3overflow with a reduced configuration surface. See NOTICE for details.
 */

package com.christophsens.snsoverflow

import com.christophsens.s3overflow.PayloadStore
import com.christophsens.s3overflow.SQS_SNS_MAX_INLINE_PAYLOAD_SIZE_BYTES

/**
 * Configures [SnsExtendedClient]. Encryption at rest is configured on the S3 bucket backing
 * [payloadStore] (SSE-S3/SSE-KMS), not here.
 */
class SnsExtendedClientConfig(
    /** Backing store for offloaded payloads, e.g. `S3BackedPayloadStore(s3Client, bucketName)`. */
    val payloadStore: PayloadStore,
    /** Messages whose body + attributes exceed this many bytes are offloaded to [payloadStore]. */
    val payloadSizeThreshold: Int = SQS_SNS_MAX_INLINE_PAYLOAD_SIZE_BYTES,
    /** When true, every message is offloaded regardless of [payloadSizeThreshold]. */
    val alwaysThroughS3: Boolean = false,
    /** Prefix prepended to the generated key of every offloaded payload. */
    s3KeyPrefix: String = "",
) {
    val s3KeyPrefix: String = validateS3KeyPrefix(s3KeyPrefix)
}
