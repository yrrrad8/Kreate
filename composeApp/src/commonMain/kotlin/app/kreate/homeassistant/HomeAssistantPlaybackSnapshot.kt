package app.kreate.homeassistant

data class HomeAssistantPlaybackSnapshot(
    val sessionAvailable: Boolean = false,
    val playbackState: HomeAssistantPlaybackState =
        HomeAssistantPlaybackState.IDLE,
    val title: String? = null,
    val artist: String? = null,
    val album: String? = null,
    val mediaId: String? = null,
    val albumId: String? = null,
    val artworkUrl: String? = null,
    val durationMs: Long = 0L,
    val positionMs: Long = 0L,
    val canPlay: Boolean = false,
    val canPause: Boolean = false,
    val canNext: Boolean = false,
    val canPrevious: Boolean = false,
    val canStop: Boolean = false,
    val canSeek: Boolean = false,
)
