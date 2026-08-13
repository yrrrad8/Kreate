package app.kreate.android.homeassistant

import android.os.Handler
import android.os.Looper
import androidx.media3.common.C
import androidx.media3.common.MediaItem
import androidx.media3.common.MediaMetadata
import androidx.media3.common.PlaybackException
import androidx.media3.common.Player
import app.kreate.android.service.player.StatefulPlayer
import app.kreate.homeassistant.HomeAssistantPlaybackController
import app.kreate.homeassistant.HomeAssistantPlaybackSnapshot
import app.kreate.homeassistant.HomeAssistantPlaybackState
import it.fast4x.rimusic.utils.resize
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.withContext

class AndroidHomeAssistantPlaybackController(
    private val player: StatefulPlayer,
    private val mainDispatcher: CoroutineDispatcher = Dispatchers.Main.immediate,
) : HomeAssistantPlaybackController {

    private val mutablePlayback =
        MutableStateFlow(HomeAssistantPlaybackSnapshot())

    override val playback: StateFlow<HomeAssistantPlaybackSnapshot> =
        mutablePlayback.asStateFlow()

    private val listener = object : Player.Listener {
        override fun onEvents(
            player: Player,
            events: Player.Events,
        ) {
            updateSnapshot()
        }

        override fun onPlayerError(error: PlaybackException) {
            updateSnapshot(
                forcedState = HomeAssistantPlaybackState.ERROR,
            )
        }
    }

    init {
        runOnPlayerThread {
            player.addListener(listener)
            updateSnapshot()
        }
    }

    override suspend fun play(): Boolean =
        executeIfAvailable(Player.COMMAND_PLAY_PAUSE) {
            player.play()
        }

    override suspend fun pause(): Boolean =
        executeIfAvailable(Player.COMMAND_PLAY_PAUSE) {
            player.pause()
        }

    override suspend fun next(): Boolean =
        executeIfAvailable(Player.COMMAND_SEEK_TO_NEXT_MEDIA_ITEM) {
            player.seekToNextMediaItem()
        }

    override suspend fun previous(): Boolean =
        executeIfAvailable(Player.COMMAND_SEEK_TO_PREVIOUS_MEDIA_ITEM) {
            player.seekToPreviousMediaItem()
        }

    override suspend fun stop(): Boolean =
        executeIfAvailable(Player.COMMAND_STOP) {
            player.stop()
        }

    override suspend fun seekTo(positionMs: Long): Boolean {
        if (positionMs < 0L) {
            return false
        }

        return executeIfAvailable(
            Player.COMMAND_SEEK_IN_CURRENT_MEDIA_ITEM
        ) {
            val duration = player.safeDuration()

            val targetPosition = if (duration != null) {
                positionMs.coerceAtMost(duration)
            } else {
                positionMs
            }

            player.seekTo(targetPosition)
        }
    }

    override fun close() {
        runOnPlayerThread {
            player.removeListener(listener)
        }
    }

    private suspend fun executeIfAvailable(
        command: Int,
        action: () -> Unit,
    ): Boolean = withContext(mainDispatcher) {
        if (!player.isCommandAvailable(command)) {
            return@withContext false
        }

        action()
        updateSnapshot()
        true
    }

    private fun updateSnapshot(
        forcedState: HomeAssistantPlaybackState? = null,
    ) {
        if (Looper.myLooper() != player.applicationLooper) {
            runOnPlayerThread {
                updateSnapshot(forcedState)
            }
            return
        }

        val hasCurrentItem =
            player.isCommandAvailable(
                Player.COMMAND_GET_CURRENT_MEDIA_ITEM
            ) && player.currentMediaItem != null

        val metadata = if (
            player.isCommandAvailable(Player.COMMAND_GET_METADATA)
        ) {
            player.mediaMetadata
        } else {
            MediaMetadata.EMPTY
        }

        val mediaItem = player.currentMediaItem

        mutablePlayback.value = HomeAssistantPlaybackSnapshot(
            sessionAvailable = hasCurrentItem,
            playbackState = forcedState
                ?: player.toHomeAssistantPlaybackState(),
            title = metadata.title?.toString(),
            artist = metadata.artist?.toString(),
            album = metadata.albumTitle?.toString(),
            mediaId = mediaItem?.mediaId
                ?.takeIf(String::isNotBlank),
            albumId = extractAlbumId(
                mediaItem = mediaItem,
                metadata = metadata,
            ),
            artworkUrl = metadata.artworkUri?.toString()?.resize(1000),
            durationMs = player.safeDuration() ?: 0L,
            positionMs = player.safePosition(),
            canPlay = player.isCommandAvailable(
                Player.COMMAND_PLAY_PAUSE
            ),
            canPause = player.isCommandAvailable(
                Player.COMMAND_PLAY_PAUSE
            ),
            canNext = player.isCommandAvailable(
                Player.COMMAND_SEEK_TO_NEXT_MEDIA_ITEM
            ),
            canPrevious = player.isCommandAvailable(
                Player.COMMAND_SEEK_TO_PREVIOUS_MEDIA_ITEM
            ),
            canStop = player.isCommandAvailable(
                Player.COMMAND_STOP
            ),
            canSeek = player.isCommandAvailable(
                Player.COMMAND_SEEK_IN_CURRENT_MEDIA_ITEM
            ),
        )
    }

    private fun Player.toHomeAssistantPlaybackState():
            HomeAssistantPlaybackState {

        return when {
            playerError != null ->
                HomeAssistantPlaybackState.ERROR

            playbackState == Player.STATE_BUFFERING ->
                HomeAssistantPlaybackState.BUFFERING

            playbackState == Player.STATE_ENDED ->
                HomeAssistantPlaybackState.STOPPED

            playbackState == Player.STATE_IDLE ->
                HomeAssistantPlaybackState.IDLE

            isPlaying ->
                HomeAssistantPlaybackState.PLAYING

            playWhenReady ->
                HomeAssistantPlaybackState.BUFFERING

            else ->
                HomeAssistantPlaybackState.PAUSED
        }
    }

    private fun Player.safeDuration(): Long? {
        if (
            !isCommandAvailable(
                Player.COMMAND_GET_CURRENT_MEDIA_ITEM
            )
        ) {
            return null
        }

        return duration.takeUnless {
            it == C.TIME_UNSET || it < 0L
        }
    }

    private fun Player.safePosition(): Long {
        if (
            !isCommandAvailable(
                Player.COMMAND_GET_CURRENT_MEDIA_ITEM
            )
        ) {
            return 0L
        }

        return currentPosition.coerceAtLeast(0L)
    }

    private fun extractAlbumId(
        mediaItem: MediaItem?,
        metadata: MediaMetadata,
    ): String? {
        /*
         * Media3 does not define a standard album ID field.
         * Kreate may store it in MediaItem.localConfiguration.tag,
         * requestMetadata.extras, or another project-specific object.
         *
         * For now, do not guess or use albumTitle as an ID.
         */
        return null
    }

    private fun runOnPlayerThread(
        action: () -> Unit,
    ) {
        if (Looper.myLooper() == player.applicationLooper) {
            action()
            return
        }

        Handler(player.applicationLooper).post(action)
    }
}