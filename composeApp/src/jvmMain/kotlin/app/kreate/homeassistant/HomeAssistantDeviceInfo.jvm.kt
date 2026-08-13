package app.kreate.homeassistant

actual fun createHomeAssistantDeviceInfo(
    deviceId: String,
    configuredName: String,
): HomeAssistantDeviceInfo {
    return HomeAssistantDeviceInfo(
        deviceId = deviceId,
        deviceName = configuredName.ifBlank {
            System.getProperty("os.name")
        },
        manufacturer = "",
        model = System.getProperty("os.arch"),
        operatingSystem = System.getProperty("os.name"),
        operatingSystemVersion = System.getProperty("os.version"),
        appVersion = "0", //APP_VERSION,
        appPackage = "app.kreate.desktop",
    )
}