package app.kreate.android.homeassistant

import android.content.Context
import android.os.Build
import app.kreate.homeassistant.HomeAssistantDeviceInfo
import app.kreate.homeassistant.HomeAssistantDeviceInfoProvider

class AndroidHomeAssistantDeviceInfoProvider(
    private val context: Context,
) : HomeAssistantDeviceInfoProvider {

    override fun getDeviceInfo(
        configuredDeviceId: String,
        configuredDeviceName: String,
    ): HomeAssistantDeviceInfo {
        val packageInfo = context.packageManager
            .getPackageInfo(context.packageName, 0)

        val manufacturer =
            Build.MANUFACTURER.orEmpty()

        val model = Build.MODEL.orEmpty()

        return HomeAssistantDeviceInfo(
            deviceId = configuredDeviceId,
            deviceName = configuredDeviceName.ifBlank {
                listOf(manufacturer, model)
                    .filter(String::isNotBlank)
                    .joinToString(" ")
                    .ifBlank { "Kreate Android" }
            },
            manufacturer = manufacturer,
            model = model,
            operatingSystem = "Android",
            operatingSystemVersion =
                Build.VERSION.RELEASE.orEmpty(),
            appName = "Kreate",
            appVersion = packageInfo.versionName
                ?: "unknown",
            appPackage = context.packageName,
        )
    }
}