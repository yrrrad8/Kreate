package app.kreate.mqtt

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.Collections
import kotlin.random.Random
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

/**
 * Integration tests against the public test.mosquitto.org broker.
 *
 * These tests require internet access and may occasionally fail because the
 * public broker can be restarted, overloaded, or temporarily unavailable.
 *
 * Do not send secrets or sensitive data to this broker.
 */
class KtorMqttClientIntegrationTest {

    private val resources = mutableListOf<TestClient>()

    @AfterTest
    fun tearDown() = runBlocking {
        resources.asReversed().forEach { resource ->
            runCatching {
                resource.client.disconnect()
            }

            resource.scope.cancel()
        }

        resources.clear()
    }

    @Test
    fun connectsAndDisconnectsOnAnonymousPort() = runBlocking {
        val testClient = createClient()

        assertEquals(
            MqttConnectionState.Disconnected,
            testClient.client.state.value,
        )

        testClient.client.connect(
            anonymousConfig(
                clientId = uniqueClientId("connect"),
            )
        )

        assertEquals(
            MqttConnectionState.Connected,
            testClient.client.state.value,
        )

        testClient.client.disconnect()

        assertEquals(
            MqttConnectionState.Disconnected,
            testClient.client.state.value,
        )
    }

    @Test
    fun canReconnectUsingTheSameClientObject() = runBlocking {
        val testClient = createClient()
        val topic = uniqueTopic("reconnect")

        testClient.client.connect(
            anonymousConfig(
                clientId = uniqueClientId("reconnect-first"),
            )
        )

        testClient.client.disconnect()

        testClient.client.connect(
            anonymousConfig(
                clientId = uniqueClientId("reconnect-second"),
            )
        )

        val received = subscribeAndAwait(
            client = testClient.client,
            scope = testClient.scope,
            topic = topic,
        ) {
            testClient.client.publish(
                topic = topic,
                payload = "after-reconnect",
                qos = MqttQos.AT_LEAST_ONCE,
            )
        }

        assertEquals(
            "after-reconnect",
            received.payloadAsString(),
        )
    }

    @Test
    fun publishesAndReceivesTextWithQos0() = runBlocking {
        val testClient = connectedClient("qos0")
        val topic = uniqueTopic("qos0")
        val expectedPayload = "Hello from Ktor MQTT QoS 0"

        val received = subscribeAndAwait(
            client = testClient.client,
            scope = testClient.scope,
            topic = topic,
            subscriptionQos = MqttQos.AT_MOST_ONCE,
        ) {
            testClient.client.publish(
                topic = topic,
                payload = expectedPayload,
                qos = MqttQos.AT_MOST_ONCE,
                retained = false,
            )
        }

        assertEquals(topic, received.topic)
        assertEquals(expectedPayload, received.payloadAsString())
        assertEquals(MqttQos.AT_MOST_ONCE, received.qos)
        assertTrue(!received.retained)
    }

    @Test
    fun publishesAndReceivesTextWithQos1() = runBlocking {
        val testClient = connectedClient("qos1")
        val topic = uniqueTopic("qos1")
        val expectedPayload = "Hello from Ktor MQTT QoS 1"

        val received = subscribeAndAwait(
            client = testClient.client,
            scope = testClient.scope,
            topic = topic,
            subscriptionQos = MqttQos.AT_LEAST_ONCE,
        ) {
            testClient.client.publish(
                topic = topic,
                payload = expectedPayload,
                qos = MqttQos.AT_LEAST_ONCE,
                retained = false,
            )
        }

        assertEquals(topic, received.topic)
        assertEquals(expectedPayload, received.payloadAsString())
        assertEquals(MqttQos.AT_LEAST_ONCE, received.qos)
        assertTrue(!received.retained)
    }

    @Test
    fun preservesBinaryPayload() = runBlocking {
        val testClient = connectedClient("binary")
        val topic = uniqueTopic("binary")

        val expectedPayload = byteArrayOf(
            0x00,
            0x01,
            0x02,
            0x7F,
            0x80.toByte(),
            0xFE.toByte(),
            0xFF.toByte(),
        )

        val received = subscribeAndAwait(
            client = testClient.client,
            scope = testClient.scope,
            topic = topic,
        ) {
            testClient.client.publish(
                topic = topic,
                payload = expectedPayload,
                qos = MqttQos.AT_LEAST_ONCE,
                retained = false,
            )
        }

        assertContentEquals(
            expectedPayload,
            received.payload,
        )
    }

    @Test
    fun receivesMessagesThroughSingleLevelWildcard() = runBlocking {
        val testClient = connectedClient("single-wildcard")
        val root = uniqueTopic("single-wildcard")
        val subscription = "$root/+/state"

        val expectedTopics = setOf(
            "$root/device-a/state",
            "$root/device-b/state",
        )

        val receivedMessages = mutableListOf<MqttMessage>()

        val collector = testClient.scope.launch {
            withTimeout(MESSAGE_TIMEOUT_MS.milliseconds) {
                testClient.client.messages
                    .filter { message ->
                        message.topic in expectedTopics
                    }
                    .collect { message ->
                        receivedMessages += message

                        if (
                            receivedMessages
                                .map { it.topic }
                                .toSet() == expectedTopics
                        ) {
                            this@withTimeout.cancel()
                        }
                    }
            }
        }

        testClient.client.subscribe(
            topic = subscription,
            qos = MqttQos.AT_LEAST_ONCE,
        )

        testClient.client.publish(
            topic = "$root/device-a/state",
            payload = "a",
            qos = MqttQos.AT_LEAST_ONCE,
        )

        testClient.client.publish(
            topic = "$root/device-b/state",
            payload = "b",
            qos = MqttQos.AT_LEAST_ONCE,
        )

        // This topic must not match "$root/+/state".
        testClient.client.publish(
            topic = "$root/device-b/info",
            payload = "ignored",
            qos = MqttQos.AT_LEAST_ONCE,
        )

        withTimeout(MESSAGE_TIMEOUT_MS.milliseconds) {
            while (
                receivedMessages
                    .map { it.topic }
                    .toSet() != expectedTopics
            ) {
                delay(25.milliseconds)
            }
        }

        collector.cancel()

        assertEquals(
            expectedTopics,
            receivedMessages.map { it.topic }.toSet(),
        )

        assertEquals(
            setOf("a", "b"),
            receivedMessages.map { it.payloadAsString() }.toSet(),
        )
    }

    @Test
    fun receivesMessagesThroughMultiLevelWildcard() = runBlocking {
        val testClient = connectedClient("multi-wildcard")
        val root = uniqueTopic("multi-wildcard")

        testClient.client.subscribe(
            topic = "$root/#",
            qos = MqttQos.AT_LEAST_ONCE,
        )

        val expected = mapOf(
            "$root/a" to "first",
            "$root/a/b" to "second",
            "$root/a/b/c" to "third",
        )

        val received = mutableMapOf<String, String>()

        val collectorJob = testClient.scope.launch {
            testClient.client.messages.collect { message ->
                if (message.topic in expected) {
                    received[message.topic] =
                        message.payloadAsString()
                }
            }
        }

        expected.forEach { (topic, payload) ->
            testClient.client.publish(
                topic = topic,
                payload = payload,
                qos = MqttQos.AT_LEAST_ONCE,
            )
        }

        withTimeout(MESSAGE_TIMEOUT_MS.milliseconds) {
            while (received.size < expected.size) {
                delay(25.milliseconds)
            }
        }

        collectorJob.cancel()

        assertEquals(expected, received)
    }

    @Test
    fun retainedMessageIsDeliveredToNewSubscriber() = runBlocking {
        val publisher = connectedClient("retained-publisher")
        val subscriber = connectedClient("retained-subscriber")

        val topic = uniqueTopic("retained")
        val payload = "retained-${uniqueSuffix()}"

        try {
            publisher.client.publish(
                topic = topic,
                payload = payload,
                qos = MqttQos.AT_LEAST_ONCE,
                retained = true,
            )

            val receivedDeferred =
                CompletableDeferred<MqttMessage>()

            val collector = subscriber.scope.launch {
                subscriber.client.messages.first { it.topic == topic }
                    .also(receivedDeferred::complete)
            }

            subscriber.client.subscribe(
                topic = topic,
                qos = MqttQos.AT_LEAST_ONCE,
            )

            val received = withTimeout(MESSAGE_TIMEOUT_MS.milliseconds) {
                receivedDeferred.await()
            }

            collector.cancel()

            assertEquals(payload, received.payloadAsString())
            assertTrue(
                received.retained,
                "A retained delivery should have retained=true",
            )
        } finally {
            // Delete the retained message from the public broker.
            publisher.client.publish(
                topic = topic,
                payload = byteArrayOf(),
                qos = MqttQos.AT_LEAST_ONCE,
                retained = true,
            )
        }
    }

    @Test
    fun deletedRetainedMessageIsNotDeliveredAgain() = runBlocking {
        val publisher = connectedClient("retained-delete-publisher")
        val topic = uniqueTopic("retained-delete")

        publisher.client.publish(
            topic = topic,
            payload = "temporary",
            qos = MqttQos.AT_LEAST_ONCE,
            retained = true,
        )

        publisher.client.publish(
            topic = topic,
            payload = byteArrayOf(),
            qos = MqttQos.AT_LEAST_ONCE,
            retained = true,
        )

        // Give the broker a moment to persist the retained deletion.
        delay(500.milliseconds)

        val subscriber = connectedClient("retained-delete-subscriber")

        subscriber.client.subscribe(
            topic = topic,
            qos = MqttQos.AT_LEAST_ONCE,
        )

        val received = runCatching {
            withTimeout(2_000.milliseconds) {
                subscriber.client.messages.first {
                    it.topic == topic &&
                            it.payload.isNotEmpty()
                }
            }
        }.getOrNull()

        assertEquals(
            null,
            received,
            "Deleted retained payload should not be delivered",
        )
    }

    @Test
    fun authenticatedConnectionWorksOnPort1884() = runBlocking {
        val testClient = createClient()

        testClient.client.connect(
            MqttConnectionConfig(
                host = BROKER_HOST,
                port = AUTHENTICATED_PORT,
                clientId = uniqueClientId("auth"),
                username = "rw",
                password = "readwrite",
                keepAliveSeconds = 20,
            )
        )

        assertEquals(
            MqttConnectionState.Connected,
            testClient.client.state.value,
        )

        val topic = uniqueTopic("authenticated")

        val received = subscribeAndAwait(
            client = testClient.client,
            scope = testClient.scope,
            topic = topic,
        ) {
            testClient.client.publish(
                topic = topic,
                payload = "authenticated-message",
                qos = MqttQos.AT_LEAST_ONCE,
            )
        }

        assertEquals(
            "authenticated-message",
            received.payloadAsString(),
        )
    }

    @Test
    fun wrongCredentialsAreRejected(): Unit = runBlocking {
        val testClient = createClient()

        val exception = assertFailsWith<MqttConnectionRejectedException> {
            testClient.client.connect(
                MqttConnectionConfig(
                    host = BROKER_HOST,
                    port = AUTHENTICATED_PORT,
                    clientId = uniqueClientId("wrong-auth"),
                    username = "rw",
                    password = "definitely-wrong-password",
                    keepAliveSeconds = 20,
                )
            )
        }

        assertTrue(
            exception.returnCode == 4 ||
                    exception.returnCode == 5,
            "Expected CONNACK code 4 or 5, got ${exception.returnCode}",
        )

        assertIs<MqttConnectionState.Error>(
            testClient.client.state.value,
        )
    }

    @Test
    fun readOnlyAccountCanSubscribe() = runBlocking {
        val subscriber = createClient()

        subscriber.client.connect(
            MqttConnectionConfig(
                host = BROKER_HOST,
                port = AUTHENTICATED_PORT,
                clientId = uniqueClientId("read-only"),
                username = "ro",
                password = "readonly",
                keepAliveSeconds = 20,
            )
        )

        val topic = uniqueTopic("readonly")

        subscriber.client.subscribe(
            topic = topic,
            qos = MqttQos.AT_LEAST_ONCE,
        )

        val publisher = createClient()

        publisher.client.connect(
            MqttConnectionConfig(
                host = BROKER_HOST,
                port = AUTHENTICATED_PORT,
                clientId = uniqueClientId("readwrite-publisher"),
                username = "rw",
                password = "readwrite",
                keepAliveSeconds = 20,
            )
        )

        val deferred = CompletableDeferred<MqttMessage>()

        val collector = subscriber.scope.launch {
            subscriber.client.messages.first { it.topic == topic }
                .also(deferred::complete)
        }

        publisher.client.publish(
            topic = topic,
            payload = "sent-to-readonly-client",
            qos = MqttQos.AT_LEAST_ONCE,
        )

        val received = withTimeout(MESSAGE_TIMEOUT_MS.milliseconds) {
            deferred.await()
        }

        collector.cancel()

        assertEquals(
            "sent-to-readonly-client",
            received.payloadAsString(),
        )
    }

    @Test
    fun writeOnlyAccountCanPublishToWriteHierarchy() = runBlocking {
        /*
         * test.mosquitto.org gives the "wo" account write access only
         * beneath the "write/#" topic hierarchy.
         */
        val subscriber = createClient()
        subscriber.client.connect(
            MqttConnectionConfig(
                host = BROKER_HOST,
                port = AUTHENTICATED_PORT,
                clientId = uniqueClientId("write-test-subscriber"),
                username = "rw",
                password = "readwrite",
                keepAliveSeconds = 20,
            )
        )

        val topic = "write/${uniqueTopic("write-only")}"

        val receivedDeferred =
            CompletableDeferred<MqttMessage>()

        val collector = subscriber.scope.launch {
            subscriber.client.messages.first { it.topic == topic }
                .also(receivedDeferred::complete)
        }

        subscriber.client.subscribe(
            topic = topic,
            qos = MqttQos.AT_LEAST_ONCE,
        )

        val publisher = createClient()
        publisher.client.connect(
            MqttConnectionConfig(
                host = BROKER_HOST,
                port = AUTHENTICATED_PORT,
                clientId = uniqueClientId("write-only"),
                username = "wo",
                password = "writeonly",
                keepAliveSeconds = 20,
            )
        )

        publisher.client.publish(
            topic = topic,
            payload = "write-only-message",
            qos = MqttQos.AT_LEAST_ONCE,
        )

        val received = withTimeout(MESSAGE_TIMEOUT_MS.milliseconds) {
            receivedDeferred.await()
        }

        collector.cancel()

        assertEquals(
            "write-only-message",
            received.payloadAsString(),
        )
    }

    @Test
    fun keepAliveKeepsIdleConnectionOpen() = runBlocking {
        val testClient = createClient()

        testClient.client.connect(
            anonymousConfig(
                clientId = uniqueClientId("keepalive"),
                keepAliveSeconds = 4,
            )
        )

        assertEquals(
            MqttConnectionState.Connected,
            testClient.client.state.value,
        )

        // Multiple PINGREQ/PINGRESP cycles should occur during this wait.
        delay(12_000.milliseconds)

        assertEquals(
            MqttConnectionState.Connected,
            testClient.client.state.value,
        )

        val topic = uniqueTopic("after-keepalive")

        val received = subscribeAndAwait(
            client = testClient.client,
            scope = testClient.scope,
            topic = topic,
        ) {
            testClient.client.publish(
                topic = topic,
                payload = "still-connected",
                qos = MqttQos.AT_LEAST_ONCE,
            )
        }

        assertEquals(
            "still-connected",
            received.payloadAsString(),
        )
    }

    @Test
    fun supportsMultipleConcurrentQos1Publishes() = runBlocking {
        val testClient = connectedClient("concurrent")
        val root = uniqueTopic("concurrent")
        val messageCount = 20

        val receivedPayloads =
            Collections.synchronizedSet(
                mutableSetOf<String>()
            )

        val collector = testClient.scope.launch {
            testClient.client.messages.collect { message ->
                if (message.topic.startsWith("$root/")) {
                    receivedPayloads += message.payloadAsString()
                }
            }
        }

        testClient.client.subscribe(
            topic = "$root/#",
            qos = MqttQos.AT_LEAST_ONCE,
        )

        coroutineScope {
            repeat(messageCount) { index ->
                launch(Dispatchers.IO) {
                    testClient.client.publish(
                        topic = "$root/$index",
                        payload = "message-$index",
                        qos = MqttQos.AT_LEAST_ONCE,
                    )
                }
            }
        }

        withTimeout((MESSAGE_TIMEOUT_MS * 2).milliseconds) {
            while (receivedPayloads.size < messageCount) {
                delay(25.milliseconds)
            }
        }

        collector.cancel()

        val expected = buildSet {
            repeat(messageCount) { index ->
                add("message-$index")
            }
        }

        assertEquals(expected, receivedPayloads)
    }

    @Test
    fun twoClientsReceiveTheSamePublishedMessage() = runBlocking {
        val publisher = connectedClient("fanout-publisher")
        val subscriberA = connectedClient("fanout-a")
        val subscriberB = connectedClient("fanout-b")

        val topic = uniqueTopic("fanout")
        val payload = "fanout-message"

        val deferredA = CompletableDeferred<MqttMessage>()
        val deferredB = CompletableDeferred<MqttMessage>()

        val collectorA = subscriberA.scope.launch {
            subscriberA.client.messages.first { it.topic == topic }
                .also(deferredA::complete)
        }

        val collectorB = subscriberB.scope.launch {
            subscriberB.client.messages.first { it.topic == topic }
                .also(deferredB::complete)
        }

        subscriberA.client.subscribe(
            topic = topic,
            qos = MqttQos.AT_LEAST_ONCE,
        )

        subscriberB.client.subscribe(
            topic = topic,
            qos = MqttQos.AT_LEAST_ONCE,
        )

        publisher.client.publish(
            topic = topic,
            payload = payload,
            qos = MqttQos.AT_LEAST_ONCE,
        )

        val receivedA = withTimeout(MESSAGE_TIMEOUT_MS.milliseconds) {
            deferredA.await()
        }

        val receivedB = withTimeout(MESSAGE_TIMEOUT_MS.milliseconds) {
            deferredB.await()
        }

        collectorA.cancel()
        collectorB.cancel()

        assertEquals(payload, receivedA.payloadAsString())
        assertEquals(payload, receivedB.payloadAsString())
    }

    @Test
    fun unicodeTopicAndPayloadArePreserved() = runBlocking {
        val testClient = connectedClient("unicode")
        val topic = uniqueTopic("unicode") + "/עברית"
        val payload = "שלום עולם – MQTT עובד ✓"

        val received = subscribeAndAwait(
            client = testClient.client,
            scope = testClient.scope,
            topic = topic,
        ) {
            testClient.client.publish(
                topic = topic,
                payload = payload,
                qos = MqttQos.AT_LEAST_ONCE,
            )
        }

        assertEquals(topic, received.topic)
        assertEquals(payload, received.payloadAsString())
    }

    @Test
    fun emptyPayloadIsSupported() = runBlocking {
        val testClient = connectedClient("empty-payload")
        val topic = uniqueTopic("empty-payload")

        val received = subscribeAndAwait(
            client = testClient.client,
            scope = testClient.scope,
            topic = topic,
        ) {
            testClient.client.publish(
                topic = topic,
                payload = byteArrayOf(),
                qos = MqttQos.AT_LEAST_ONCE,
                retained = false,
            )
        }

        assertContentEquals(
            byteArrayOf(),
            received.payload,
        )
    }

    @Test
    fun publishBeforeConnectFails(): Unit = runBlocking {
        val testClient = createClient()

        assertFailsWith<IllegalStateException> {
            testClient.client.publish(
                topic = uniqueTopic("not-connected"),
                payload = "should-fail",
                qos = MqttQos.AT_LEAST_ONCE,
            )
        }
    }

    @Test
    fun subscribeBeforeConnectFails(): Unit = runBlocking {
        val testClient = createClient()

        assertFailsWith<IllegalStateException> {
            testClient.client.subscribe(
                topic = uniqueTopic("not-connected"),
                qos = MqttQos.AT_LEAST_ONCE,
            )
        }
    }

    @Test
    fun publishRejectsWildcardTopicNames(): Unit = runBlocking {
        val testClient = connectedClient("invalid-topic")

        assertFailsWith<IllegalArgumentException> {
            testClient.client.publish(
                topic = "${uniqueTopic("invalid")}/+",
                payload = "invalid",
            )
        }

        assertFailsWith<IllegalArgumentException> {
            testClient.client.publish(
                topic = "${uniqueTopic("invalid")}/#",
                payload = "invalid",
            )
        }
    }

    @Test
    fun duplicateClientIdDisconnectsThePreviousClient() = runBlocking {
        val duplicateClientId = uniqueClientId("duplicate")

        val first = createClient()
        first.client.connect(
            anonymousConfig(
                clientId = duplicateClientId,
                keepAliveSeconds = 4,
            )
        )

        val second = createClient()
        second.client.connect(
            anonymousConfig(
                clientId = duplicateClientId,
                keepAliveSeconds = 4,
            )
        )

        assertEquals(
            MqttConnectionState.Connected,
            second.client.state.value,
        )

        withTimeout(MESSAGE_TIMEOUT_MS.milliseconds) {
            while (
                first.client.state.value ==
                MqttConnectionState.Connected
            ) {
                delay(50.milliseconds)
            }
        }

        assertNotEquals(
            MqttConnectionState.Connected,
            first.client.state.value,
        )
    }

    private suspend fun connectedClient(
        label: String,
    ): TestClient {
        val testClient = createClient()

        testClient.client.connect(
            anonymousConfig(
                clientId = uniqueClientId(label),
            )
        )

        assertEquals(
            MqttConnectionState.Connected,
            testClient.client.state.value,
        )

        return testClient
    }

    private fun createClient(): TestClient {
        val scope = CoroutineScope(
            SupervisorJob() + Dispatchers.IO
        )

        val client = KtorMqttClient(
            dispatcher = Dispatchers.IO,
            parentScope = scope,
            acknowledgementTimeoutMs = ACK_TIMEOUT_MS,
            connectionTimeoutMs = CONNECTION_TIMEOUT_MS,
        )

        return TestClient(
            client = client,
            scope = scope,
        ).also(resources::add)
    }

    private suspend fun subscribeAndAwait(
        client: MqttClient,
        scope: CoroutineScope,
        topic: String,
        subscriptionQos: MqttQos =
            MqttQos.AT_LEAST_ONCE,
        expectedTopic: String = topic,
        action: suspend () -> Unit,
    ): MqttMessage {
        val received = CompletableDeferred<MqttMessage>()

        val collectorJob = scope.launch {
            client.messages.first { message ->
                message.topic == expectedTopic
            }
                .also(received::complete)
        }

        /*
         * Start collecting before SUBSCRIBE. SharedFlow has no replay, and a
         * retained message may arrive immediately after SUBACK.
         */
        client.subscribe(
            topic = topic,
            qos = subscriptionQos,
        )

        action()

        return try {
            withTimeout(MESSAGE_TIMEOUT_MS.milliseconds) {
                received.await()
            }
        } finally {
            collectorJob.cancel()
        }
    }

    private fun anonymousConfig(
        clientId: String,
        keepAliveSeconds: Int = 20,
    ): MqttConnectionConfig {
        return MqttConnectionConfig(
            host = BROKER_HOST,
            port = ANONYMOUS_PORT,
            clientId = clientId,
            keepAliveSeconds = keepAliveSeconds,
            cleanSession = true,
        )
    }

    private fun uniqueClientId(
        label: String,
    ): String {
        /*
         * MQTT 3.1.1 brokers commonly support longer IDs, but keeping this
         * relatively short improves compatibility.
         */
        return "krt-$label-${uniqueSuffix()}"
            .take(60)
    }

    private fun uniqueTopic(
        label: String,
    ): String {
        /*
         * A private-looking prefix is not private. test.mosquitto.org is a
         * public broker, and other users can subscribe to these topics.
         */
        return "kreate-mqtt-tests/$RUN_ID/$label/${uniqueSuffix()}"
    }

    private fun uniqueSuffix(): String {
        return buildString {
            append(
                Random.nextLong()
                    .toULong()
                    .toString(16)
            )
            append('-')
            append(
                System.nanoTime()
                    .toULong()
                    .toString(16)
            )
        }
    }

    private data class TestClient(
        val client: KtorMqttClient,
        val scope: CoroutineScope,
    )

    private companion object {
        const val BROKER_HOST = "test.mosquitto.org"
        const val ANONYMOUS_PORT = 1883
        const val AUTHENTICATED_PORT = 1884

        const val CONNECTION_TIMEOUT_MS = 20_000L
        const val ACK_TIMEOUT_MS = 15_000L
        const val MESSAGE_TIMEOUT_MS = 20_000L

        val RUN_ID: String =
            Random.nextLong()
                .toULong()
                .toString(16)
    }
}