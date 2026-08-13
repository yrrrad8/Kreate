package app.kreate.homeassistant

import kotlinx.coroutines.flow.StateFlow

interface HomeAssistantPlaybackController {

    val playback: StateFlow<HomeAssistantPlaybackSnapshot>

    suspend fun play(): Boolean

    suspend fun pause(): Boolean

    suspend fun next(): Boolean

    suspend fun previous(): Boolean

    suspend fun stop(): Boolean

    suspend fun seekTo(positionMs: Long): Boolean

    fun close()
}