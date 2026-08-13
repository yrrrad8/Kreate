package app.kreate.homeassistant

interface HomeAssistantDeviceInfoProvider {
    fun getDeviceInfo(
        configuredDeviceId: String,
        configuredDeviceName: String,
    ): HomeAssistantDeviceInfo
}