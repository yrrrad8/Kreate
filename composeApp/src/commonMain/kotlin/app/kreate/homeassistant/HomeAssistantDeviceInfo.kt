package app.kreate.homeassistant

data class HomeAssistantDeviceInfo(
    val deviceId: String,
    val deviceName: String,
    val manufacturer: String,
    val model: String,
    val operatingSystem: String,
    val operatingSystemVersion: String,
    val appName: String = "Kreate",
    val appVersion: String,
    val appPackage: String,
)