package app.kreate.homeassistant

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class HomeAssistantInfoPayload(
    @SerialName("device_id")
    val deviceId: String,

    @SerialName("device_name")
    val deviceName: String,

    val manufacturer: String,
    val model: String,

    @SerialName("os_name")
    val operatingSystem: String,

    @SerialName("os_version")
    val operatingSystemVersion: String,

    @SerialName("app_name")
    val appName: String,

    @SerialName("app_version")
    val appVersion: String,

    @SerialName("app_package")
    val appPackage: String,

    @SerialName("protocol_version")
    val protocolVersion: Int = 1,

    val permissions: HomeAssistantPermissionsPayload,
)

@Serializable
data class HomeAssistantPermissionsPayload(
    @SerialName("play_pause")
    val playPause: Boolean,

    val next: Boolean,
    val previous: Boolean,
    val stop: Boolean,
    val seek: Boolean,
)

@Serializable
data class HomeAssistantStatePayload(
    @SerialName("session_found")
    val sessionFound: Boolean,

    @SerialName("state")
    val playbackState: String,

    val title: String? = null,
    val artist: String? = null,
    val album: String? = null,

    @SerialName("media_id")
    val mediaId: String? = null,

    @SerialName("album_id")
    val albumId: String? = null,

    @SerialName("artwork_url")
    val artworkUrl: String? = null,

    @SerialName("duration_ms")
    val durationMs: Long = 0L,

    @SerialName("position_ms")
    val positionMs: Long = 0L,

//    @SerialName("available_actions")
//    val availableActions: List<String> = emptyList(),

    @SerialName("allow_play_pause")
    val allowPlayPause: Boolean = false,

    @SerialName("allow_next")
    val allowNext: Boolean = false,

    @SerialName("allow_previous")
    val allowPrevious: Boolean = false,

    @SerialName("allow_seek")
    val allowSeek: Boolean = false,

    @SerialName("allow_stop")
    val allowStop: Boolean = false,

    @SerialName("updated_at")
    val updatedAt: Long = 0L
)

@Serializable
data class HomeAssistantCommandPayload(
    val id: String? = null,
    val command: String,

    @SerialName("position_ms")
    val positionMs: Long? = null,

    @SerialName("position")
    val position: Float? = null
)

@Serializable
data class HomeAssistantCommandResultPayload(
    val id: String? = null,
    val command: String,
    val success: Boolean,
    val error: String? = null,
)