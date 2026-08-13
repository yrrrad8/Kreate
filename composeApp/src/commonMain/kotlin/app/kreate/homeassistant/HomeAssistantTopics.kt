package app.kreate.homeassistant

data class HomeAssistantTopics(
    val deviceId: String,
) {
    private val root: String =
        "kreate-ha/devices/$deviceId"

    val info: String = "$root/info"
    val availability: String = "$root/availability"
    val state: String = "$root/state"
    val command: String = "$root/command"
    val commandResult: String = "$root/command_result"
}