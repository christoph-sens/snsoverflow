# snsoverflow

[![Maven Central](https://img.shields.io/maven-central/v/com.christoph-sens/snsoverflow)](https://central.sonatype.com/artifact/com.christoph-sens/snsoverflow)
[![CI](https://github.com/christoph-sens/snsoverflow/actions/workflows/ci.yml/badge.svg)](https://github.com/christoph-sens/snsoverflow/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Kotlin port of [amazon-sns-java-extended-client-lib](https://github.com/awslabs/amazon-sns-java-extended-client-lib):
transparently offloads SNS message bodies that exceed the configured size threshold to S3, built on
[aws-sdk-kotlin](https://github.com/awslabs/aws-sdk-kotlin) and [s3overflow](https://github.com/christoph-sens/s3overflow)
(a Kotlin reimplementation of `payload-offloading-java-common-lib-for-aws`, the library the original depends on).

This is a derivative work of the original under the Apache License, Version 2.0 — see
[NOTICE](NOTICE) for exactly which parts were ported and what was changed.

Part of a family: [s3overflow](https://github.com/christoph-sens/s3overflow) (payload store) · [sqsoverflow](https://github.com/christoph-sens/sqsoverflow) (SQS client) · **snsoverflow** (SNS client).

> **Message size limit:** SNS topics accept 256 KiB by default, which matches the default
> `payloadSizeThreshold` (`SNS_DEFAULT_MAX_MESSAGE_SIZE_BYTES`). If you raise the topic's `MaximumMessageSize` attribute (up to 1 MiB),
> set `payloadSizeThreshold` to the same value.

## Why a port

The original ships two large classes (`AmazonSNSExtendedClient` for `SnsClient`,
`AmazonSNSExtendedAsyncClient` for `SnsAsyncClient`) that are almost entirely pass-through
boilerplate to the wrapped SNS client, plus a configuration class inherited from
`payload-offloading-java-common-lib-for-aws`.

| Original | Here |
|---|---|
| Separate `AmazonSNSExtendedClient`/`AmazonSNSExtendedAsyncClient`, large `AmazonSNSExtendedClientBase` of pure pass-through methods | `aws-sdk-kotlin`'s `SnsClient` is already `suspend`-based, so one `SnsExtendedClient` covers both; Kotlin interface delegation (`SnsClient by snsClient`) replaces the entire pass-through base class — only `publish`/`publishBatch`, which contain the real offload logic, are overridden |
| `payloadoffloading-common`'s `PayloadStore`/`S3BackedPayloadStore`/`S3Dao`/`Util` | [s3overflow](https://github.com/christoph-sens/s3overflow) |
| `SNSExtendedClientConfiguration` extends `PayloadStorageConfiguration` (S3 client, `ObjectCannedACL`, `ServerSideEncryptionStrategy`) | `SnsExtendedClientConfig` takes a `PayloadStore` directly; ACL/CSE-style config is dropped — encryption is configured on the S3 bucket itself, same simplification s3overflow and sqsoverflow already made |
| Per-message `"S3Key"` attribute override for pinning an exact S3 key | Dropped in favor of the config-level `s3KeyPrefix`, matching [sqsoverflow](https://github.com/christoph-sens/sqsoverflow)'s `SqsExtendedClient` for a consistent configuration surface |
| Only `publish` is offload-aware (the original predates SNS's `PublishBatch` API) | `publishBatch` is offload-aware too, offloading only the entries that need it — the same batch pattern sqsoverflow already uses for `sendMessageBatch` |
| 6 main classes, ~2500 lines | 3 files, ~170 lines |

**Note:** dropped compared to the original: client-side `ObjectCannedACL`/`ServerSideEncryptionStrategy`
configuration (bucket-level SSE-S3/SSE-KMS instead) and the per-message `"S3Key"` attribute
override. Core behavior — threshold-based offloading, `alwaysThroughS3`, `s3KeyPrefix`,
rejecting the multi-protocol JSON message structure, message-attribute validation — is preserved.
Unlike SQS, SNS is fire-and-forget: there's no receipt handle, so no receive/delete/cleanup side
to this client. A subscriber resolves the pointer itself — for an SNS topic fanning out to an SQS
queue with raw message delivery enabled, that's [sqsoverflow](https://github.com/christoph-sens/sqsoverflow)'s
`SqsExtendedClient`, which recognizes the same reserved attribute name.

## Usage

```kotlin
val snsClient = SnsClient.fromEnvironment { region = "eu-central-1" }
val s3Client = S3Client.fromEnvironment { region = "eu-central-1" }

val extendedClient =
    SnsExtendedClient(
        snsClient,
        SnsExtendedClientConfig(payloadStore = S3BackedPayloadStore(s3Client, bucketName = "my-payload-bucket")),
    )

extendedClient.publish(PublishRequest { topicArn = myTopicArn; message = largePayload })
```

`SnsExtendedClient` implements `SnsClient`, so it's a drop-in replacement wherever a plain
`aws-sdk-kotlin` `SnsClient` is expected.

## Migrating from amazon-sns-java-extended-client-lib

snsoverflow is **not wire-compatible** with the Java library: the S3 pointer format differs.
Subscribers using the Java SQS extended client or the Java payload-offloading library cannot
resolve messages published by snsoverflow, and vice versa. Switch publishers and subscribers of a
topic at the same time.

```java
// Before (Java, amazon-sns-java-extended-client-lib)
SNSExtendedClientConfiguration config = new SNSExtendedClientConfiguration()
    .withPayloadSupportEnabled(s3Client, "my-payload-bucket");
SnsClient client = new AmazonSNSExtendedClient(SnsClient.builder().build(), config);
```

```kotlin
// After (Kotlin, snsoverflow)
val client = SnsExtendedClient(
    SnsClient.fromEnvironment(),
    SnsExtendedClientConfig(payloadStore = S3BackedPayloadStore(s3Client, bucketName = "my-payload-bucket")),
)
```

Client-side encryption, canned ACL and the per-message `"S3Key"` override have no equivalent; configure
SSE-S3/SSE-KMS on the bucket and use `s3KeyPrefix` instead.

## Build

```bash
./gradlew build
```

## Integration tests

`./gradlew integrationTest` runs [SnsExtendedClientIntegrationTest](src/integrationTest/kotlin/com/christophsens/snsoverflow/SnsExtendedClientIntegrationTest.kt)
against real SNS, SQS, and S3 APIs via [Testcontainers](https://testcontainers.com)/[Floci](https://github.com/floci-io/floci)
(a free, MIT-licensed local AWS emulator; used instead of LocalStack, whose community edition
now requires an auth token). Since SNS is fire-and-forget, the test subscribes an SQS queue to
the topic with raw message delivery enabled and inspects what SNS actually delivered: a message
under the threshold arrives unchanged with no object created in S3, and a message over the
threshold arrives as only a pointer while the payload lands in S3. Requires Docker; not part of
`./gradlew build`/`check`.

## Installation

```kotlin
dependencies {
    implementation("com.christoph-sens:snsoverflow:<version>")
}
```

```xml
<dependency>
  <groupId>com.christoph-sens</groupId>
  <artifactId>snsoverflow</artifactId>
  <version><version></version>
</dependency>
```

### Verifying a release

Every file published to Maven Central (jars, POM, Gradle module metadata) has a signed
[build provenance attestation](https://docs.github.com/en/actions/security-for-github-actions/using-artifact-attestations)
proving it was built by this repository's release workflow from the tagged commit. Verify a
downloaded file with the GitHub CLI:

```bash
gh attestation verify snsoverflow-<version>.jar --repo christoph-sens/snsoverflow
```

Releases published before provenance attestations were introduced have no attestation.

## Releasing (maintainers)

Releases are published to Maven Central by the [release workflow](.github/workflows/release.yml)
using the [Vanniktech Maven Publish plugin](https://github.com/vanniktech/gradle-maven-publish-plugin).
The version comes from the Git tag; there is no version to bump in the build file.

```bash
git tag v1.2.3
git push origin v1.2.3
```

The workflow builds and tests the tag, then waits for manual approval in the `maven-central`
environment before signing and publishing. The publish job attests the build provenance of the
published files before uploading them, then creates a GitHub release with generated notes and
the published jars attached. Maven Central releases are immutable: fix mistakes with a new patch release.
Running the workflow manually (`workflow_dispatch`) is a dry run that never publishes.

This library depends on [s3overflow](https://github.com/christoph-sens/s3overflow). When releasing
both, release s3overflow first and wait for Dependabot to bump it here before tagging this repo.

## Contributing

Contributions are welcome — see [CONTRIBUTING](CONTRIBUTING.md). This project follows the
[Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

This project is licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).

It is a Kotlin port of
[amazon-sns-java-extended-client-lib](https://github.com/awslabs/amazon-sns-java-extended-client-lib)
(Copyright 2010-2020 Amazon.com, Inc. or its affiliates, also licensed under Apache-2.0) and
therefore a derivative work under that license. See [NOTICE](NOTICE) for exactly which files
and logic were ported, what was changed, and the per-file copyright headers in
`src/main/kotlin/com/christophsens/snsoverflow`.
