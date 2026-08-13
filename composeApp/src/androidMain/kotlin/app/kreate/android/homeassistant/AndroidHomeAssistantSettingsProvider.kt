package app.kreate.android.homeassistant

import androidx.compose.runtime.snapshotFlow
import app.kreate.android.Preferences
import app.kreate.homeassistant.HomeAssistantSettings
import app.kreate.homeassistant.HomeAssistantSettingsProvider
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.stateIn

class AndroidHomeAssistantSettingsProvider(
    scope: CoroutineScope,
) : HomeAssistantSettingsProvider {

    override val settings: StateFlow<HomeAssistantSettings> =
        snapshotFlow {
            HomeAssistantSettings(
                enabled =
                    Preferences.HOME_ASSISTANT_ENABLED.value,
                host =
                    Preferences.HOME_ASSISTANT_MQTT_HOST.value,
                port =
                    Preferences.HOME_ASSISTANT_MQTT_PORT.value,
                username =
                    Preferences.HOME_ASSISTANT_MQTT_USERNAME.value,
                password =
                    Preferences.HOME_ASSISTANT_MQTT_PASSWORD.value,
                deviceId =
                    Preferences.HOME_ASSISTANT_DEVICE_ID.value,
                deviceName =
                    Preferences.HOME_ASSISTANT_DEVICE_NAME.value,
                allowPlayPause =
                    Preferences.HOME_ASSISTANT_ALLOW_PLAY_PAUSE.value,
                allowNext =
                    Preferences.HOME_ASSISTANT_ALLOW_NEXT.value,
                allowPrevious =
                    Preferences.HOME_ASSISTANT_ALLOW_PREVIOUS.value,
                allowStop =
                    Preferences.HOME_ASSISTANT_ALLOW_STOP.value,
                allowSeek =
                    Preferences.HOME_ASSISTANT_ALLOW_SEEK.value,
            )
        }.stateIn(
            scope = scope,
            started = SharingStarted.Eagerly,
            initialValue = HomeAssistantSettings(),
        )
}