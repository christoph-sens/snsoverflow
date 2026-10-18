package com.christophsens.snsoverflow

import aws.sdk.kotlin.services.sns.SnsClient
import aws.sdk.kotlin.services.sns.model.ListTopicsRequest
import aws.sdk.kotlin.services.sns.model.ListTopicsResponse
import com.christophsens.s3overflow.PayloadStore
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import java.lang.reflect.Proxy
import kotlin.test.Test

/** Stands in for an SDK interface: [existing] is known at compile time, [addedLater] only in a newer version. */
interface Operations {
    suspend fun existing(): String

    suspend fun addedLater(): String
}

class OffloadingProxyTest {
    private val delegate =
        object : Operations {
            override suspend fun existing() = "delegate existing"

            override suspend fun addedLater(): String {
                delay(1_000)
                return "delegate addedLater"
            }
        }

    /** Behaves like a class compiled against the older interface: it has no implementation of [Operations.addedLater]. */
    private val compiledAgainstOldVersion =
        Proxy.newProxyInstance(Operations::class.java.classLoader, arrayOf(Operations::class.java)) { _, method, _ ->
            if (method.name == "existing") "offloading existing" else throw AbstractMethodError(method.name)
        } as Operations

    private val proxy = offloadingProxy(compiledAgainstOldVersion, delegate, offloadedOperations = setOf("existing"))

    @Test
    fun `routes offloaded operations to the offloading implementation`() =
        runTest {
            assertThat(proxy.existing()).isEqualTo("offloading existing")
        }

    @Test
    fun `forwards operations the offloading implementation does not know to the delegate`() =
        runTest {
            assertThat(proxy.addedLater()).isEqualTo("delegate addedLater")
            assertThat(currentTime).isEqualTo(1_000)
        }

    @Test
    fun `rethrows the original exception`() {
        val failure = IllegalStateException("boom")
        val failing =
            offloadingProxy(
                compiledAgainstOldVersion,
                object : Operations {
                    override suspend fun existing() = error("unused")

                    override suspend fun addedLater(): String = throw failure
                },
                offloadedOperations = setOf("existing"),
            )

        assertThatThrownBy { runTest { failing.addedLater() } }.isSameAs(failure)
    }

    @Test
    fun `implements equals, hashCode and toString without touching the targets`() {
        val other = offloadingProxy(compiledAgainstOldVersion, delegate, offloadedOperations = setOf("existing"))

        assertThat(proxy).isEqualTo(proxy).isNotEqualTo(other)
        assertThat(proxy.hashCode()).isEqualTo(System.identityHashCode(proxy))
        assertThat(proxy.toString()).startsWith("SnsExtendedClient(delegate=")
    }

    @Test
    fun `offloaded operations exist on SnsClient`() {
        val snsClientOperations = SnsClient::class.java.methods.map { it.name }.toSet()

        assertThat(snsClientOperations).containsAll(OFFLOADED_OPERATIONS)
    }

    @Test
    fun `SnsExtendedClient forwards other operations to the wrapped client`() =
        runTest {
            val snsClient = mockk<SnsClient>()
            coEvery { snsClient.listTopics(any<ListTopicsRequest>()) } returns ListTopicsResponse { nextToken = "next" }
            val client = SnsExtendedClient(snsClient, SnsExtendedClientConfig(mockk<PayloadStore>()))

            val response = client.listTopics(ListTopicsRequest {})

            assertThat(response.nextToken).isEqualTo("next")
            coVerify { snsClient.listTopics(any<ListTopicsRequest>()) }
        }
}
