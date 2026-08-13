package app.kreate.homeassistant

import app.kreate.mqtt.MqttClient
import app.kreate.mqtt.MqttConnectionConfig
import app.kreate.mqtt.MqttConnectionState
import app.kreate.mqtt.MqttQos
import app.kreate.mqtt.MqttWill
import app.kreate.mqtt.publish
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlin.time.Duration.Companion.milliseconds

class HomeAssistantBridge(
    private val mqttClient: MqttClient,
    private val settingsProvider: HomeAssistantSettingsProvider,
    private val playbackController: HomeAssistantPlaybackController,
    private val deviceInfoProvider: HomeAssistantDeviceInfoProvider,
    private val scope: CoroutineScope,
) {

    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
        explicitNulls = true
    }

    private var settingsJob: Job? = null
    private var playbackJob: Job? = null
    private var messagesJob: Job? = null
    private var connectionWatchJob: Job? = null
    private var activeSettings: HomeAssistantSettings? = null

    fun start() {
        if (settingsJob != null) {
            return
        }

        settingsJob = scope.launch {
            settingsProvider.settings.collectLatest { settings ->
                stopActiveConnection()

                if (!settings.isConfigured) {
                    return@collectLatest
                }

                activeSettings = settings
                maintainConnection(settings)
            }
        }
    }

    suspend fun stop() {
        settingsJob?.cancel()
        settingsJob = null

        stopActiveConnection()
    }

    private suspend fun maintainConnection(
        settings: HomeAssistantSettings,
    ) {
        var retryDelayMs = INITIAL_RETRY_DELAY_MS

        while (
            settingsProvider.settings.value == settings &&
            settings.isConfigured
        ) {
            try {
                connect(settings)

                mqttClient.state.collect { state ->
                    when (state) {
                        MqttConnectionState.Connected -> {
                            retryDelayMs = INITIAL_RETRY_DELAY_MS
                        }

                        MqttConnectionState.Disconnected -> {
                            throw ConnectionLostException(
                                "MQTT connection disconnected"
                            )
                        }

                        is MqttConnectionState.Error -> {
                            throw ConnectionLostException(
                                state.message,
                                state.cause,
                            )
                        }

                        MqttConnectionState.Connecting -> Unit
                    }
                }
            } catch (exception: CancellationException) {
                throw exception
            } catch (_: Throwable) {
                stopConnectionJobs()

                runCatching {
                    mqttClient.disconnect()
                }

                delay(retryDelayMs.milliseconds)

                retryDelayMs = (
                        retryDelayMs * 2
                        ).coerceAtMost(MAX_RETRY_DELAY_MS)
            }
        }
    }

    private suspend fun connect(
        settings: HomeAssistantSettings,
    ) {
        val topics = HomeAssistantTopics(settings.deviceId)

        mqttClient.connect(
            MqttConnectionConfig(
                host = settings.host,
                port = settings.port,
                clientId = "kreate-${settings.deviceId}",
                username = settings.username
                    .takeIf(String::isNotBlank),
                password = settings.password
                    .takeIf(String::isNotBlank),
                cleanSession = true,
                keepAliveSeconds = 30,
                will = MqttWill(
                    topic = topics.availability,
                    payload = "offline",
                    qos = MqttQos.AT_LEAST_ONCE,
                    retained = true,
                ),
            )
        )

        mqttClient.subscribe(
            topic = topics.command,
            qos = MqttQos.AT_LEAST_ONCE,
        )

        publishInfo(settings, topics)

        mqttClient.publish(
            topic = topics.availability,
            payload = "online",
            qos = MqttQos.AT_LEAST_ONCE,
            retained = true,
        )

        startPlaybackPublisher(settings, topics)
        startCommandListener(settings, topics)
    }

    private fun startPlaybackPublisher(
        settings: HomeAssistantSettings,
        topics: HomeAssistantTopics,
    ) {
        playbackJob?.cancel()

        playbackJob = scope.launch {
            playbackController.playback.collectLatest { snapshot ->
                val payload = createStatePayload(
                    settings = settings,
                    snapshot = snapshot,
                )

                mqttClient.publish(
                    topic = topics.state,
                    payload = json.encodeToString(payload),
                    qos = MqttQos.AT_LEAST_ONCE,
                    retained = true,
                )
            }
        }
    }

    private fun startCommandListener(
        settings: HomeAssistantSettings,
        topics: HomeAssistantTopics,
    ) {
        messagesJob?.cancel()

        messagesJob = scope.launch {
            mqttClient.messages.collect { message ->
                if (message.topic != topics.command) {
                    return@collect
                }

                processCommand(
                    settings = settings,
                    topics = topics,
                    rawPayload = message.payloadAsString(),
                )
            }
        }
    }

    private suspend fun processCommand(
        settings: HomeAssistantSettings,
        topics: HomeAssistantTopics,
        rawPayload: String,
    ) {
        val command = try {
            json.decodeFromString<HomeAssistantCommandPayload>(
                rawPayload
            )
        } catch (exception: Throwable) {
            if (exception is CancellationException) {
                throw exception
            }

            publishCommandResult(
                topics = topics,
                result = HomeAssistantCommandResultPayload(
                    command = "unknown",
                    success = false,
                    error = "Invalid command payload",
                ),
            )
            return
        }

        val execution = runCatching {
            executeCommand(
                settings = settings,
                command = command,
            )
        }

        publishCommandResult(
            topics = topics,
            result = HomeAssistantCommandResultPayload(
                id = command.id,
                command = command.command,
                success = execution.getOrDefault(false),
                error = execution.exceptionOrNull()?.message,
            ),
        )
    }

    private suspend fun executeCommand(
        settings: HomeAssistantSettings,
        command: HomeAssistantCommandPayload,
    ): Boolean {
        val snapshot = playbackController.playback.value

        return when (command.command.lowercase()) {
            "play" -> {
                require(settings.allowPlayPause) {
                    "Play control is disabled"
                }
                require(snapshot.canPlay) {
                    "Play is not currently available"
                }
                playbackController.play()
            }

            "pause" -> {
                require(settings.allowPlayPause) {
                    "Pause control is disabled"
                }
                require(snapshot.canPause) {
                    "Pause is not currently available"
                }
                playbackController.pause()
            }

            "next" -> {
                require(settings.allowNext) {
                    "Next control is disabled"
                }
                require(snapshot.canNext) {
                    "Next is not currently available"
                }
                playbackController.next()
            }

            "previous" -> {
                require(settings.allowPrevious) {
                    "Previous control is disabled"
                }
                require(snapshot.canPrevious) {
                    "Previous is not currently available"
                }
                playbackController.previous()
            }

            "stop" -> {
                require(settings.allowStop) {
                    "Stop control is disabled"
                }
                require(snapshot.canStop) {
                    "Stop is not currently available"
                }
                playbackController.stop()
            }

            "seek" -> {
                require(settings.allowSeek) {
                    "Seek control is disabled"
                }
                require(snapshot.canSeek) {
                    "Seek is not currently available"
                }

                val pos = command.positionMs ?: command.position?.times(1000L)?.toLong()
                requireNotNull(pos) {
                    "position_ms or position is required for seek"
                }

                require(pos >= 0L) {
                        "position_ms or position must not be negative"
                }

                playbackController.seekTo(pos)
            }

            else -> error(
                "Unsupported command: ${command.command}"
            )
        }
    }

    private suspend fun publishInfo(
        settings: HomeAssistantSettings,
        topics: HomeAssistantTopics,
    ) {
        val info = deviceInfoProvider.getDeviceInfo(
            configuredDeviceId = settings.deviceId,
            configuredDeviceName = settings.deviceName,
        )

        val payload = HomeAssistantInfoPayload(
            deviceId = info.deviceId,
            deviceName = info.deviceName,
            manufacturer = info.manufacturer,
            model = info.model,
            operatingSystem = info.operatingSystem,
            operatingSystemVersion =
                info.operatingSystemVersion,
            appName = info.appName,
            appVersion = info.appVersion,
            appPackage = info.appPackage,
            permissions = HomeAssistantPermissionsPayload(
                playPause = settings.allowPlayPause,
                next = settings.allowNext,
                previous = settings.allowPrevious,
                stop = settings.allowStop,
                seek = settings.allowSeek,
            ),
        )

        mqttClient.publish(
            topic = topics.info,
            payload = json.encodeToString(payload),
            qos = MqttQos.AT_LEAST_ONCE,
            retained = true,
        )
    }

    private suspend fun publishCommandResult(
        topics: HomeAssistantTopics,
        result: HomeAssistantCommandResultPayload,
    ) {
        mqttClient.publish(
            topic = topics.commandResult,
            payload = json.encodeToString(result),
            qos = MqttQos.AT_LEAST_ONCE,
            retained = false,
        )
    }

    private fun createStatePayload(
        settings: HomeAssistantSettings,
        snapshot: HomeAssistantPlaybackSnapshot,
    ): HomeAssistantStatePayload {
        return HomeAssistantStatePayload(
            sessionFound = snapshot.sessionAvailable,
            playbackState =
                snapshot.playbackState.name.lowercase(),
            title = snapshot.title,
            artist = snapshot.artist,
            album = snapshot.album,
            mediaId = snapshot.mediaId,
            albumId = snapshot.albumId,
            artworkUrl = snapshot.artworkUrl,
            durationMs = snapshot.durationMs,
            positionMs = snapshot.positionMs,
            allowPlayPause = settings.allowPlayPause,
            allowNext = settings.allowNext,
            allowPrevious = settings.allowPrevious,
            allowSeek = settings.allowSeek,
            allowStop = settings.allowPrevious,
            updatedAt = System.currentTimeMillis()
        )
    }

    private suspend fun stopActiveConnection() {
        activeSettings = null
        stopConnectionJobs()

        runCatching {
            mqttClient.disconnect()
        }
    }

    private fun stopConnectionJobs() {
        playbackJob?.cancel()
        messagesJob?.cancel()
        connectionWatchJob?.cancel()

        playbackJob = null
        messagesJob = null
        connectionWatchJob = null
    }

    private class ConnectionLostException(
        message: String,
        cause: Throwable? = null,
    ) : Exception(message, cause)

    private companion object {
        const val INITIAL_RETRY_DELAY_MS = 2_000L
        const val MAX_RETRY_DELAY_MS = 60_000L
    }
}