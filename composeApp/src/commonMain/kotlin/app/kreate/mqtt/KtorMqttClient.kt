package app.kreate.mqtt

import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration.Companion.milliseconds

class KtorMqttClient(
    private val dispatcher: CoroutineDispatcher,
    parentScope: CoroutineScope,
    private val acknowledgementTimeoutMs: Long = 10_000L,
    private val connectionTimeoutMs: Long = 15_000L,
) : MqttClient {

    private val lifecycleMutex = Mutex()
    private val writeMutex = Mutex()
    private val pendingMutex = Mutex()
    private val packetIdMutex = Mutex()

    private val mutableState =
        MutableStateFlow<MqttConnectionState>(
            MqttConnectionState.Disconnected
        )

    override val state =
        mutableState.asStateFlow()

    private val mutableMessages = MutableSharedFlow<MqttMessage>(
        extraBufferCapacity = 64,
    )

    override val messages: Flow<MqttMessage> =
        mutableMessages.asSharedFlow()

    private val parentJob = parentScope.coroutineContext[Job]

    private var connectionJob: Job? = null
    private var connectionScope: CoroutineScope? = null

    private var selectorManager: SelectorManager? = null
    private var socket: Socket? = null
    private var input: ByteReadChannel? = null
    private var output: ByteWriteChannel? = null

    private var readerJob: Job? = null
    private var keepAliveJob: Job? = null

    private var currentConfig: MqttConnectionConfig? = null
    private var nextPacketId = 1

    private val pendingPublishAcknowledgements =
        mutableMapOf<Int, CompletableDeferred<Unit>>()

    private val pendingSubscribeAcknowledgements =
        mutableMapOf<Int, CompletableDeferred<List<Int>>>()

    private var pendingPingResponse:
            CompletableDeferred<Unit>? = null

    override suspend fun connect(
        config: MqttConnectionConfig,
    ) {
        validateConfig(config)

        lifecycleMutex.withLock {
            closeConnectionLocked(
                sendDisconnect = true,
                finalState = MqttConnectionState.Disconnected,
            )

            mutableState.value = MqttConnectionState.Connecting

            try {
                val supervisor = SupervisorJob(parentJob)
                val scope = CoroutineScope(
                    dispatcher + supervisor
                )

                connectionJob = supervisor
                connectionScope = scope
                currentConfig = config

                val selector = SelectorManager(dispatcher)
                selectorManager = selector

                val connectedSocket = withTimeout(
                    connectionTimeoutMs.milliseconds
                ) {
                    aSocket(selector)
                        .tcp()
                        .connect(
                            hostname = config.host,
                            port = config.port,
                        )
                }

                socket = connectedSocket
                input = connectedSocket.openReadChannel()
                output = connectedSocket.openWriteChannel(
                    autoFlush = false
                )

                writeRawPacket(
                    MqttPacketCodec.connect(config)
                )

                val connAck = withTimeout(
                    connectionTimeoutMs.milliseconds
                ) {
                    MqttPacketCodec.readPacket(
                        requireNotNull(input)
                    )
                }

                MqttPacketCodec.parseConnAck(connAck)

                mutableState.value =
                    MqttConnectionState.Connected

                readerJob = scope.launch {
                    readerLoop()
                }

                if (config.keepAliveSeconds > 0) {
                    keepAliveJob = scope.launch {
                        keepAliveLoop(config.keepAliveSeconds)
                    }
                }
            } catch (exception: Throwable) {
                val mqttException = when (exception) {
                    is MqttException -> exception
                    is CancellationException -> exception
                    else -> MqttException(
                        message = exception.message
                            ?: "MQTT connection failed",
                        cause = exception,
                    )
                }

                closeConnectionLocked(
                    sendDisconnect = false,
                    finalState = MqttConnectionState.Error(
                        message = mqttException.message
                            ?: "MQTT connection failed",
                        cause = mqttException,
                    ),
                )

                throw mqttException
            }
        }
    }

    override suspend fun publish(
        topic: String,
        payload: ByteArray,
        qos: MqttQos,
        retained: Boolean,
    ) {
        ensureConnected()

        if (qos == MqttQos.AT_MOST_ONCE) {
            writeRawPacket(
                MqttPacketCodec.publish(
                    topic = topic,
                    payload = payload,
                    qos = qos,
                    retained = retained,
                    packetId = null,
                )
            )
            return
        }

        val packetId = allocatePacketId()
        val acknowledgement = CompletableDeferred<Unit>()

        pendingMutex.withLock {
            pendingPublishAcknowledgements[packetId] =
                acknowledgement
        }

        try {
            writeRawPacket(
                MqttPacketCodec.publish(
                    topic = topic,
                    payload = payload,
                    qos = qos,
                    retained = retained,
                    packetId = packetId,
                )
            )

            withTimeout(acknowledgementTimeoutMs.milliseconds) {
                acknowledgement.await()
            }
        } finally {
            pendingMutex.withLock {
                pendingPublishAcknowledgements.remove(packetId)
            }
        }
    }

    override suspend fun subscribe(
        topic: String,
        qos: MqttQos,
    ) {
        ensureConnected()

        val packetId = allocatePacketId()
        val acknowledgement =
            CompletableDeferred<List<Int>>()

        pendingMutex.withLock {
            pendingSubscribeAcknowledgements[packetId] =
                acknowledgement
        }

        try {
            writeRawPacket(
                MqttPacketCodec.subscribe(
                    packetId = packetId,
                    topic = topic,
                    qos = qos,
                )
            )

            val returnCodes = withTimeout(
                acknowledgementTimeoutMs.milliseconds
            ) {
                acknowledgement.await()
            }

            if (returnCodes.isEmpty()) {
                throw MqttException(
                    "Broker returned an empty SUBACK"
                )
            }

            if (returnCodes.any { it == 0x80 }) {
                throw MqttException(
                    "Broker rejected subscription to $topic"
                )
            }

            if (returnCodes.any { it !in setOf(0, 1, 2) }) {
                throw MqttException(
                    "Broker returned an invalid SUBACK code"
                )
            }
        } finally {
            pendingMutex.withLock {
                pendingSubscribeAcknowledgements.remove(packetId)
            }
        }
    }

    override suspend fun disconnect() {
        lifecycleMutex.withLock {
            closeConnectionLocked(
                sendDisconnect = true,
                finalState = MqttConnectionState.Disconnected,
            )
        }
    }

    private suspend fun readerLoop() {
        val channel = requireNotNull(input)

        try {
            while (connectionScope?.isActive == true) {
                val packet = MqttPacketCodec.readPacket(channel)
                handleIncomingPacket(packet)
            }
        } catch (_: CancellationException) {
            // Expected during a normal disconnect.
        } catch (exception: Throwable) {
            handleConnectionFailure(exception)
        }
    }

    private suspend fun handleIncomingPacket(
        packet: MqttRawPacket,
    ) {
        when (packet.type) {
            MqttPacketType.PUBLISH ->
                handleIncomingPublish(packet)

            MqttPacketType.PUBACK ->
                handlePubAck(packet)

            MqttPacketType.SUBACK ->
                handleSubAck(packet)

            MqttPacketType.PINGRESP ->
                handlePingResponse(packet)

            else -> {
                // Other MQTT packet types are not required by this client.
            }
        }
    }

    private suspend fun handleIncomingPublish(
        packet: MqttRawPacket,
    ) {
        val publish = MqttPacketCodec.parsePublish(packet)

        if (publish.qos == MqttQos.AT_LEAST_ONCE) {
            writeRawPacket(
                MqttPacketCodec.pubAck(
                    requireNotNull(publish.packetId)
                )
            )
        }

        mutableMessages.emit(
            MqttMessage(
                topic = publish.topic,
                payload = publish.payload,
                qos = publish.qos,
                retained = publish.retained,
                duplicate = publish.duplicate,
            )
        )
    }

    private suspend fun handlePubAck(
        packet: MqttRawPacket,
    ) {
        val packetId = MqttPacketCodec.parsePubAck(packet)

        val pending = pendingMutex.withLock {
            pendingPublishAcknowledgements.remove(packetId)
        }

        pending?.complete(Unit)
    }

    private suspend fun handleSubAck(
        packet: MqttRawPacket,
    ) {
        val subAck = MqttPacketCodec.parseSubAck(packet)

        val pending = pendingMutex.withLock {
            pendingSubscribeAcknowledgements.remove(
                subAck.packetId
            )
        }

        pending?.complete(subAck.returnCodes)
    }

    private fun handlePingResponse(
        packet: MqttRawPacket,
    ) {
        if (packet.flags != 0 || packet.body.isNotEmpty()) {
            throw MqttException("Malformed PINGRESP packet")
        }

        pendingPingResponse?.complete(Unit)
        pendingPingResponse = null
    }

    private suspend fun keepAliveLoop(
        keepAliveSeconds: Int,
    ) {
        val intervalMs =
            (keepAliveSeconds * 1_000L / 2L)
                .coerceAtLeast(1_000L)

        try {
            while (connectionScope?.isActive == true) {
                delay(intervalMs.milliseconds)

                ensureConnected()

                val response = CompletableDeferred<Unit>()
                pendingPingResponse = response

                writeRawPacket(
                    MqttPacketCodec.pingRequest()
                )

                withTimeout(
                    (keepAliveSeconds * 1_000L).milliseconds
                ) {
                    response.await()
                }

                pendingPingResponse = null
            }
        } catch (_: CancellationException) {
            // Expected during disconnect.
        } catch (exception: Throwable) {
            handleConnectionFailure(
                MqttException(
                    message = "MQTT keep-alive failed",
                    cause = exception,
                )
            )
        }
    }

    private suspend fun handleConnectionFailure(
        exception: Throwable,
    ) {
        lifecycleMutex.withLock {
            if (
                mutableState.value ==
                MqttConnectionState.Disconnected
            ) {
                return
            }

            closeConnectionLocked(
                sendDisconnect = false,
                finalState = MqttConnectionState.Error(
                    message = exception.message
                        ?: "MQTT connection lost",
                    cause = exception,
                ),
            )
        }
    }

    private suspend fun writeRawPacket(
        packet: ByteArray,
    ) {
        writeMutex.withLock {
            val channel = output
                ?: throw MqttException(
                    "MQTT output channel is unavailable"
                )

            try {
                channel.writeFully(packet)
                channel.flush()
            } catch (exception: Throwable) {
                throw MqttException(
                    message = "Failed to write MQTT packet",
                    cause = exception,
                )
            }
        }
    }

    private suspend fun closeConnectionLocked(
        sendDisconnect: Boolean,
        finalState: MqttConnectionState,
    ) {
        val wasConnected =
            mutableState.value == MqttConnectionState.Connected

        if (sendDisconnect && wasConnected && output != null) {
            runCatching {
                writeRawPacket(
                    MqttPacketCodec.disconnect()
                )
            }
        }

        readerJob?.cancel()
        keepAliveJob?.cancel()

        readerJob = null
        keepAliveJob = null

        pendingPingResponse?.cancel()
        pendingPingResponse = null

        failPendingOperations(
            MqttException("MQTT connection closed")
        )

        runCatching {
            socket?.close()
        }

        runCatching {
            selectorManager?.close()
        }

        connectionScope?.cancel()
        connectionJob?.cancel()

        input = null
        output = null
        socket = null
        selectorManager = null
        connectionScope = null
        connectionJob = null
        currentConfig = null

        mutableState.value = finalState
    }

    private suspend fun failPendingOperations(
        exception: Throwable,
    ) {
        pendingMutex.withLock {
            pendingPublishAcknowledgements
                .values
                .forEach { it.completeExceptionally(exception) }

            pendingSubscribeAcknowledgements
                .values
                .forEach { it.completeExceptionally(exception) }

            pendingPublishAcknowledgements.clear()
            pendingSubscribeAcknowledgements.clear()
        }
    }

    private suspend fun allocatePacketId(): Int {
        return packetIdMutex.withLock {
            repeat(65_535) {
                val candidate = nextPacketId

                nextPacketId =
                    if (nextPacketId == 65_535) {
                        1
                    } else {
                        nextPacketId + 1
                    }

                val inUse = pendingMutex.withLock {
                    candidate in pendingPublishAcknowledgements ||
                            candidate in
                            pendingSubscribeAcknowledgements
                }

                if (!inUse) {
                    return@withLock candidate
                }
            }

            throw MqttException(
                "No MQTT packet identifiers are available"
            )
        }
    }

    private fun ensureConnected() {
        check(
            mutableState.value == MqttConnectionState.Connected
        ) {
            "MQTT client is not connected"
        }
    }

    private fun validateConfig(
        config: MqttConnectionConfig,
    ) {
        require(config.host.isNotBlank()) {
            "MQTT host must not be blank"
        }

        require(config.port in 1..65_535) {
            "MQTT port must be between 1 and 65535"
        }

        require(config.clientId.isNotBlank()) {
            "MQTT client ID must not be blank"
        }

        require(config.keepAliveSeconds in 0..65_535) {
            "MQTT keep-alive must be between 0 and 65535"
        }

        config.will?.let { will ->
            require(will.topic.isNotBlank()) {
                "MQTT Will topic must not be blank"
            }
        }

        require(
            config.password == null ||
                    config.username != null
        ) {
            "MQTT password requires a username"
        }
    }
}