package app.kreate.homeassistant

data class HomeAssistantSettings(
    val enabled: Boolean = false,
    val host: String = "",
    val port: Int = 1883,
    val username: String = "",
    val password: String = "",
    val deviceId: String = "",
    val deviceName: String = "",
    val allowPlayPause: Boolean = true,
    val allowNext: Boolean = true,
    val allowPrevious: Boolean = true,
    val allowStop: Boolean = false,
    val allowSeek: Boolean = false,
) {
    val isConfigured: Boolean
        get() = enabled &&
                host.isNotBlank() &&
                port in 1..65535 &&
                deviceId.isNotBlank()
}