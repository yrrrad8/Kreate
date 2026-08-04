package app.kreate.mqtt

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.readByte
import io.ktor.utils.io.readFully

internal object MqttPacketType {
    const val CONNECT = 1
    const val CONNACK = 2
    const val PUBLISH = 3
    const val PUBACK = 4
    const val SUBSCRIBE = 8
    const val SUBACK = 9
    const val PINGREQ = 12
    const val PINGRESP = 13
    const val DISCONNECT = 14
}

internal data class MqttRawPacket(
    val type: Int,
    val flags: Int,
    val body: ByteArray,
)

internal object MqttPacketCodec {

    private const val MAX_REMAINING_LENGTH = 268_435_455

    fun connect(
        config: MqttConnectionConfig,
    ): ByteArray {
        require(config.clientId.isNotBlank()) {
            "MQTT client ID must not be blank"
        }

        require(config.keepAliveSeconds in 0..65_535) {
            "MQTT keep-alive must be between 0 and 65535 seconds"
        }

        val variableHeader = MqttByteWriter().apply {
            writeUtf8("MQTT")
            writeByte(4) // MQTT 3.1.1 protocol level
        }

        var connectFlags = 0

        if (config.cleanSession) {
            connectFlags = connectFlags or 0x02
        }

        val will = config.will

        if (will != null) {
            connectFlags = connectFlags or 0x04
            connectFlags = connectFlags or (will.qos.value shl 3)

            if (will.retained) {
                connectFlags = connectFlags or 0x20
            }
        }

        if (config.password != null) {
            connectFlags = connectFlags or 0x40
        }

        if (config.username != null) {
            connectFlags = connectFlags or 0x80
        }

        variableHeader.writeByte(connectFlags)
        variableHeader.writeUnsignedShort(config.keepAliveSeconds)

        val payload = MqttByteWriter().apply {
            writeUtf8(config.clientId)

            if (will != null) {
                writeUtf8(requireNotNull(will.topic))
                writeBinary(will.payload)
            }

            config.username?.let(::writeUtf8)
            config.password?.let(::writeUtf8)
        }

        return packet(
            firstByte = MqttPacketType.CONNECT shl 4,
            body = variableHeader.toByteArray() + payload.toByteArray(),
        )
    }

    fun publish(
        topic: String,
        payload: ByteArray,
        qos: MqttQos,
        retained: Boolean,
        packetId: Int?,
        duplicate: Boolean = false,
    ): ByteArray {
        requireValidTopicName(topic)

        val qosValue = qos.value

        if (qos == MqttQos.AT_MOST_ONCE) {
            require(packetId == null) {
                "QoS 0 PUBLISH must not contain a packet identifier"
            }
        } else {
            requireValidPacketId(packetId)
        }

        var flags = qosValue shl 1

        if (retained) {
            flags = flags or 0x01
        }

        if (duplicate) {
            flags = flags or 0x08
        }

        val body = MqttByteWriter().apply {
            writeUtf8(topic)

            if (qos != MqttQos.AT_MOST_ONCE) {
                writeUnsignedShort(requireNotNull(packetId))
            }

            writeBytes(payload)
        }

        return packet(
            firstByte = (MqttPacketType.PUBLISH shl 4) or flags,
            body = body.toByteArray(),
        )
    }

    fun subscribe(
        packetId: Int,
        topic: String,
        qos: MqttQos,
    ): ByteArray {
        requireValidPacketId(packetId)
        requireValidTopicFilter(topic)

        val body = MqttByteWriter().apply {
            writeUnsignedShort(packetId)
            writeUtf8(topic)
            writeByte(qos.value)
        }

        return packet(
            firstByte = 0x82,
            body = body.toByteArray(),
        )
    }

    fun pubAck(
        packetId: Int,
    ): ByteArray {
        requireValidPacketId(packetId)

        return byteArrayOf(
            0x40,
            0x02,
            ((packetId ushr 8) and 0xFF).toByte(),
            (packetId and 0xFF).toByte(),
        )
    }

    fun pingRequest(): ByteArray =
        byteArrayOf(0xC0.toByte(), 0x00)

    fun disconnect(): ByteArray =
        byteArrayOf(0xE0.toByte(), 0x00)

    suspend fun readPacket(
        input: ByteReadChannel,
    ): MqttRawPacket {
        val firstByte = input.readByte().toInt() and 0xFF
        val remainingLength = readRemainingLength(input)

        val body = ByteArray(remainingLength)

        if (remainingLength > 0) {
            input.readFully(body)
        }

        return MqttRawPacket(
            type = firstByte ushr 4,
            flags = firstByte and 0x0F,
            body = body,
        )
    }

    fun parseConnAck(
        packet: MqttRawPacket,
    ) {
        if (packet.type != MqttPacketType.CONNACK) {
            throw MqttException(
                "Expected CONNACK, received packet type ${packet.type}"
            )
        }

        if (packet.flags != 0) {
            throw MqttException("Invalid CONNACK flags: ${packet.flags}")
        }

        if (packet.body.size != 2) {
            throw MqttException(
                "Invalid CONNACK length: ${packet.body.size}"
            )
        }

        val acknowledgeFlags = packet.body[0].toInt() and 0xFF
        val returnCode = packet.body[1].toInt() and 0xFF

        if (acknowledgeFlags and 0xFE != 0) {
            throw MqttException(
                "Invalid CONNACK acknowledge flags: $acknowledgeFlags"
            )
        }

        if (returnCode != 0) {
            throw MqttConnectionRejectedException(
                returnCode = returnCode,
                message = connAckErrorMessage(returnCode),
            )
        }
    }

    fun parsePublish(
        packet: MqttRawPacket,
    ): ParsedPublish {
        require(packet.type == MqttPacketType.PUBLISH) {
            "Packet is not a PUBLISH packet"
        }

        val duplicate = packet.flags and 0x08 != 0
        val retained = packet.flags and 0x01 != 0
        val qosValue = (packet.flags ushr 1) and 0x03

        if (qosValue == 3) {
            throw MqttProtocolException("Invalid PUBLISH QoS value")
        }

        if (qosValue == 2) {
            throw MqttProtocolException(
                "Incoming MQTT QoS 2 messages are not supported"
            )
        }

        val qos = MqttQos.fromValue(qosValue)

        val reader = MqttByteReader(packet.body)
        val topic = reader.readUtf8()

        requireValidTopicName(topic)

        val packetId = if (qos != MqttQos.AT_MOST_ONCE) {
            reader.readUnsignedShort().also(::requireValidPacketId)
        } else {
            null
        }

        return ParsedPublish(
            topic = topic,
            payload = reader.readRemainingBytes(),
            qos = qos,
            retained = retained,
            duplicate = duplicate,
            packetId = packetId,
        )
    }

    fun parsePubAck(
        packet: MqttRawPacket,
    ): Int {
        if (packet.type != MqttPacketType.PUBACK) {
            throw MqttException("Packet is not PUBACK")
        }

        if (packet.flags != 0 || packet.body.size != 2) {
            throw MqttException("Malformed PUBACK packet")
        }

        return MqttByteReader(packet.body)
            .readUnsignedShort()
            .also(::requireValidPacketId)
    }

    fun parseSubAck(
        packet: MqttRawPacket,
    ): ParsedSubAck {
        if (packet.type != MqttPacketType.SUBACK) {
            throw MqttException("Packet is not SUBACK")
        }

        if (packet.flags != 0 || packet.body.size < 3) {
            throw MqttException("Malformed SUBACK packet")
        }

        val reader = MqttByteReader(packet.body)
        val packetId = reader
            .readUnsignedShort()
            .also(::requireValidPacketId)

        val returnCodes = reader
            .readRemainingBytes()
            .map { it.toInt() and 0xFF }

        return ParsedSubAck(
            packetId = packetId,
            returnCodes = returnCodes,
        )
    }

    private fun packet(
        firstByte: Int,
        body: ByteArray,
    ): ByteArray {
        require(body.size <= MAX_REMAINING_LENGTH) {
            "MQTT packet body is too large"
        }

        return byteArrayOf(firstByte.toByte()) +
                encodeRemainingLength(body.size) +
                body
    }

    private fun encodeRemainingLength(
        length: Int,
    ): ByteArray {
        require(length in 0..MAX_REMAINING_LENGTH)

        var value = length
        val encoded = ArrayList<Byte>(4)

        do {
            var digit = value % 128
            value /= 128

            if (value > 0) {
                digit = digit or 0x80
            }

            encoded += digit.toByte()
        } while (value > 0)

        return encoded.toByteArray()
    }

    private suspend fun readRemainingLength(
        input: ByteReadChannel,
    ): Int {
        var multiplier = 1
        var value = 0
        var bytesRead = 0

        do {
            if (bytesRead == 4) {
                throw MqttException(
                    "Malformed MQTT Remaining Length"
                )
            }

            val digit = input.readByte().toInt() and 0xFF
            bytesRead++

            value += (digit and 0x7F) * multiplier

            if (digit and 0x80 == 0) {
                return value
            }

            multiplier *= 128
        } while (true)
    }

    private fun connAckErrorMessage(
        returnCode: Int,
    ): String {
        return when (returnCode) {
            1 -> "MQTT connection rejected: unsupported protocol version"
            2 -> "MQTT connection rejected: client ID rejected"
            3 -> "MQTT connection rejected: broker unavailable"
            4 -> "MQTT connection rejected: invalid username or password"
            5 -> "MQTT connection rejected: not authorized"
            else -> "MQTT connection rejected with code $returnCode"
        }
    }
}

internal data class ParsedPublish(
    val topic: String,
    val payload: ByteArray,
    val qos: MqttQos,
    val retained: Boolean,
    val duplicate: Boolean,
    val packetId: Int?,
)

internal data class ParsedSubAck(
    val packetId: Int,
    val returnCodes: List<Int>,
)

private class MqttByteWriter(
    initialCapacity: Int = 64,
) {
    private var buffer = ByteArray(initialCapacity)
    private var size = 0

    fun writeByte(
        value: Int,
    ) {
        ensureCapacity(1)
        buffer[size++] = value.toByte()
    }

    fun writeUnsignedShort(
        value: Int,
    ) {
        require(value in 0..65_535) {
            "Value must fit into an unsigned short"
        }

        ensureCapacity(2)
        buffer[size++] = ((value ushr 8) and 0xFF).toByte()
        buffer[size++] = (value and 0xFF).toByte()
    }

    fun writeUtf8(
        value: String,
    ) {
        val encoded = value.encodeToByteArray()
        validateMqttUtf8(value, encoded)

        writeUnsignedShort(encoded.size)
        writeBytes(encoded)
    }

    fun writeBinary(
        value: ByteArray,
    ) {
        require(value.size <= 65_535) {
            "MQTT binary field is too large"
        }

        writeUnsignedShort(value.size)
        writeBytes(value)
    }

    fun writeBytes(
        value: ByteArray,
    ) {
        ensureCapacity(value.size)
        value.copyInto(
            destination = buffer,
            destinationOffset = size,
        )
        size += value.size
    }

    fun toByteArray(): ByteArray =
        buffer.copyOf(size)

    private fun ensureCapacity(
        extra: Int,
    ) {
        val required = size + extra

        if (required <= buffer.size) {
            return
        }

        var newSize = buffer.size.coerceAtLeast(1)

        while (newSize < required) {
            newSize *= 2
        }

        buffer = buffer.copyOf(newSize)
    }
}

private class MqttByteReader(
    private val bytes: ByteArray,
) {
    private var position = 0

    fun readUnsignedShort(): Int {
        requireAvailable(2)

        val high = bytes[position++].toInt() and 0xFF
        val low = bytes[position++].toInt() and 0xFF

        return (high shl 8) or low
    }

    fun readUtf8(): String {
        val length = readUnsignedShort()
        requireAvailable(length)

        val valueBytes = bytes.copyOfRange(
            fromIndex = position,
            toIndex = position + length,
        )
        position += length

        val value = valueBytes.decodeToString(
            throwOnInvalidSequence = true,
        )

        validateMqttUtf8(value, valueBytes)
        return value
    }

    fun readRemainingBytes(): ByteArray {
        val result = bytes.copyOfRange(position, bytes.size)
        position = bytes.size
        return result
    }

    private fun requireAvailable(
        count: Int,
    ) {
        if (count < 0 || position + count > bytes.size) {
            throw MqttException("Malformed MQTT packet")
        }
    }
}

private fun requireValidPacketId(
    packetId: Int?,
) {
    require(packetId != null && packetId in 1..65_535) {
        "MQTT packet identifier must be between 1 and 65535"
    }
}

private fun requireValidTopicName(
    topic: String,
) {
    require(topic.isNotEmpty()) {
        "MQTT topic name must not be empty"
    }

    require('\u0000' !in topic) {
        "MQTT topic must not contain a null character"
    }

    require('+' !in topic && '#' !in topic) {
        "MQTT topic names must not contain wildcard characters"
    }

    val bytes = topic.encodeToByteArray()

    require(bytes.size <= 65_535) {
        "MQTT topic is too long"
    }
}

private fun requireValidTopicFilter(
    topic: String,
) {
    require(topic.isNotEmpty()) {
        "MQTT topic filter must not be empty"
    }

    require('\u0000' !in topic) {
        "MQTT topic filter must not contain a null character"
    }

    val bytes = topic.encodeToByteArray()

    require(bytes.size <= 65_535) {
        "MQTT topic filter is too long"
    }
}

private fun validateMqttUtf8(
    value: String,
    encoded: ByteArray,
) {
    require(encoded.size <= 65_535) {
        "MQTT UTF-8 field is too large"
    }

    require('\u0000' !in value) {
        "MQTT UTF-8 field must not contain a null character"
    }

    value.forEach { character ->
        require(character !in '\uD800'..'\uDFFF') {
            "MQTT UTF-8 field contains an invalid surrogate"
        }
    }
}