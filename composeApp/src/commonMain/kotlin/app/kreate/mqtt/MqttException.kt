package app.kreate.mqtt

open class MqttException(
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)

class MqttConnectionRejectedException(
    val returnCode: Int,
    message: String,
) : MqttException(message)

class MqttProtocolException(
    message: String,
    cause: Throwable? = null,
) : MqttException(message, cause)