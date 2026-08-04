package app.kreate.mqtt

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow

interface MqttClient {
    val state: StateFlow<MqttConnectionState>

    /**
     * All incoming PUBLISH messages received from the broker.
     */
    val messages: Flow<MqttMessage>

    suspend fun connect(config: MqttConnectionConfig)

    suspend fun disconnect()

    suspend fun publish(
        topic: String,
        payload: ByteArray,
        qos: MqttQos = MqttQos.AT_MOST_ONCE,
        retained: Boolean = false,
    )

    suspend fun subscribe(
        topic: String,
        qos: MqttQos = MqttQos.AT_LEAST_ONCE,
    )
}

suspend fun MqttClient.publish(
    topic: String,
    payload: String,
    qos: MqttQos = MqttQos.AT_MOST_ONCE,
    retained: Boolean = false,
) {
    publish(
        topic = topic,
        payload = payload.encodeToByteArray(),
        qos = qos,
        retained = retained,
    )
}

data class MqttConnectionConfig(
    val host: String,
    val port: Int = 1883,
    val clientId: String,
    val username: String? = null,
    val password: String? = null,
    val cleanSession: Boolean = true,
    val keepAliveSeconds: Int = 30,
    val will: MqttWill? = null,
)

data class MqttWill(
    val topic: String,
    val payload: ByteArray,
    val qos: MqttQos = MqttQos.AT_LEAST_ONCE,
    val retained: Boolean = false,
)

fun MqttWill(
    topic: String,
    payload: String,
    qos: MqttQos = MqttQos.AT_LEAST_ONCE,
    retained: Boolean = false,
): MqttWill {
    return MqttWill(
        topic = topic,
        payload = payload.encodeToByteArray(),
        qos = qos,
        retained = retained,
    )
}

data class MqttMessage(
    val topic: String,
    val payload: ByteArray,
    val qos: MqttQos,
    val retained: Boolean,
    val duplicate: Boolean,
) {
    fun payloadAsString(): String =
        payload.decodeToString(throwOnInvalidSequence = false)
}

enum class MqttQos(
    val value: Int,
) {
    AT_MOST_ONCE(0),
    AT_LEAST_ONCE(1);

    companion object {
        fun fromValue(value: Int): MqttQos {
            return when (value) {
                0 -> AT_MOST_ONCE
                1 -> AT_LEAST_ONCE
                else -> throw MqttException(
                    "Unsupported MQTT QoS value: $value"
                )
            }
        }
    }
}

sealed interface MqttConnectionState {
    data object Disconnected : MqttConnectionState
    data object Connecting : MqttConnectionState
    data object Connected : MqttConnectionState

    data class Error(
        val message: String,
        val cause: Throwable? = null,
    ) : MqttConnectionState
}