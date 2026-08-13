package app.kreate.homeassistant


sealed interface MqttConnectionState {

    data object Disconnected : MqttConnectionState

    data object Connecting : MqttConnectionState

    data object Connected : MqttConnectionState

    data class Error(
        val message: String,
        val cause: Throwable? = null,
    ) : MqttConnectionState
}