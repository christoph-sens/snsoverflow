# snsoverflow

Kotlin port of [amazon-sns-java-extended-client-lib](https://github.com/awslabs/amazon-sns-java-extended-client-lib):
transparently offloads SNS message bodies that exceed the 256 KB limit to S3, built on
[aws-sdk-kotlin](https://github.com/awslabs/aws-sdk-kotlin) and [s3overflow](https://github.com/christoph-sens/s3overflow)
(a Kotlin reimplementation of `payload-offloading-java-common-lib-for-aws`, the library the original depends on).

This is a derivative work of the original under the Apache License, Version 2.0 — see
[NOTICE](NOTICE) for exactly which parts were ported and what was changed.

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

Once published to Maven Central:

```kotlin
dependencies {
    implementation("com.christoph-sens:snsoverflow:0.1.0")
}
```

```xml
<dependency>
  <groupId>com.christoph-sens</groupId>
  <artifactId>snsoverflow</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Publishing (maintainers)

Publishing uses the [Vanniktech Maven Publish plugin](https://github.com/vanniktech/gradle-maven-publish-plugin)
against Sonatype's Central Publishing Portal. This requires a Central account with the
`com.christoph-sens` namespace verified (via a DNS TXT record on `christoph-sens.com`) and a GPG
signing key. Set the following in `~/.gradle/gradle.properties` (never commit these):

```properties
mavenCentralUsername=...
mavenCentralPassword=...
signing.keyId=...
signing.password=...
signing.secretKeyRingFile=...
```

Then bump `version` in [build.gradle.kts](build.gradle.kts) and run:

```bash
./gradlew publishToMavenCentral
```

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
