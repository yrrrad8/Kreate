package app.kreate.homeassistant

data class HomeAssistantMqttConfig(
    val enabled: Boolean,
    val host: String,
    val port: Int,
    val username: String,
    val password: String,
    val deviceId: String,
    val deviceName: String,
    val allowPlayPause: Boolean,
    val allowNext: Boolean,
    val allowPrevious: Boolean,
    val allowStop: Boolean,
    val allowSeek: Boolean,
) {
    val isValid: Boolean
        get() = enabled &&
                host.isNotBlank() &&
                port in 1..65535 &&
                deviceId.isNotBlank()
}