package app.kreate.homeassistant

import kotlinx.coroutines.flow.StateFlow

interface HomeAssistantSettingsProvider {
    val settings: StateFlow<HomeAssistantSettings>
}