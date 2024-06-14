package com.melexis.tema.ritdb_inflight_data_generator

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper
import kotlinx.coroutines.*
import org.eclipse.paho.mqttv5.client.*
import org.eclipse.paho.mqttv5.common.MqttException
import org.eclipse.paho.mqttv5.common.MqttMessage
import org.eclipse.paho.mqttv5.common.packet.MqttProperties
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.statements.StatementContext
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.time.Instant
import kotlin.math.log10
import kotlin.math.pow
import kotlin.system.measureNanoTime
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds


@JsonSerialize(using = DetailedRitdbBodyDecoder::class)
data class DetailedRitdbBody(
    val metadata: Map<String, Any>,
    val rows: List<RitDbRow>
)

@JsonSerialize(using = RitDbRowEncoder::class)
data class RitDbRow(
    val sequence: Long,
    val entityID: Long,
    val indexID: Long,
    val name: String,
    val value: Any?,
    val value2: String?
)

class NoneColumnType: ColumnType<Any>() {
    override fun sqlType(): String = "NONE"
    override fun valueFromDB(value: Any): Any = value
}

fun Table.none(name: String): Column<Any> = registerColumn(name, NoneColumnType())

object RitDbTable: Table("ritdb1") {
    val sequence = long("sequence")
    val entityID = long("entityID")
    val indexID = long("indexID")
    val name = text("name")
    val value = none("value").nullable()
    val value2 = text("value2").nullable()
}

//Create encoder for RitDbRow as array
class RitDbRowEncoder : StdSerializer<RitDbRow>(RitDbRow::class.java) {
    override fun serialize(value: RitDbRow, gen: JsonGenerator, provider: SerializerProvider) {
        require(gen is CBORGenerator)

        gen.writeStartArray()
        gen.writeNumber(value.sequence)
        gen.writeNumber(value.entityID)
        gen.writeNumber(value.indexID)
        gen.writeString(value.name)
        gen.writeObject(value.value)
        gen.writeString(value.value2)
        gen.writeEndArray()
    }
}

class DetailedRitdbBodyDecoder : StdSerializer<DetailedRitdbBody>(DetailedRitdbBody::class.java) {
    override fun serialize(value: DetailedRitdbBody, gen: JsonGenerator, provider: SerializerProvider) {
        require(gen is CBORGenerator)

        gen.writeStartArray()

        gen.writeStartArray()
        gen.writeTag(19) //Specified to be a named omap
        gen.writeString("metadata")

        for ((k, v) in value.metadata.entries) {
            gen.writeString(k)
            gen.writeObject(v)
        }

        gen.writeEndArray()

        gen.writeStartArray()
        value.rows.forEach {
            gen.writeObject(it)
        }
        gen.writeEndArray()

        gen.writeEndArray()
    }
}

object MyCallback: MqttCallback {
    override fun disconnected(p0: MqttDisconnectResponse?) {
//        println("Disconnected")
    }

    override fun mqttErrorOccurred(p0: MqttException?) {
//        println("Error occurred")
    }

    override fun messageArrived(p0: String?, p1: MqttMessage?) {
//        println("Message arrived")
    }

    override fun deliveryComplete(p0: IMqttToken?) {
//        println("Delivery complete")
    }

    override fun connectComplete(p0: Boolean, p1: String?) {
//        println("Connected")
    }

    override fun authPacketArrived(p0: Int, p1: MqttProperties?) {
//        println("Auth packet arrived")
    }
}

@OptIn(ExperimentalUnsignedTypes::class, ExperimentalStdlibApi::class)
fun main() = runBlocking {

    val mqttClient = MqttAsyncClient("tcp://localhost:1883", "ritdb_inflight_data_generator")

    var significantTime = Duration.ZERO
    var callCounts = 0L
    val bodySizes = mutableListOf<Int>()

    var serializationTime = Duration.ZERO

    mqttClient.setCallback(MyCallback)
    mqttClient.connect()

    val cbor = CBORMapper()

    Database.connect("jdbc:sqlite:${System.getenv("SQLITE_PATH")}")

    newSuspendedTransaction {
        addLogger(object : SqlLogger {
            override fun log(context: StatementContext, transaction: Transaction) {
                //println("${Instant.now()}: SQL ${context.expandArgs(transaction)}")
            }
        })

        val startInstant = Instant.now().also { println("Start time: $it") }

        val batchSize = lazy { 2500.also { println("Batch size: $it") } }
        var offset = 0L

        while(true) {
            val resultRows =RitDbTable.selectAll().orderBy(RitDbTable.sequence).limit(batchSize.value, offset).toList()
            offset += resultRows.size

            if (offset % 200000 == 0L)
                println("Offset: ${offset}")

            if (resultRows.isEmpty()) break

            val rows = resultRows.map { resultRow ->
                RitDbRow(
                    resultRow[RitDbTable.sequence],
                    resultRow[RitDbTable.entityID],
                    resultRow[RitDbTable.indexID],
                    resultRow[RitDbTable.name],
                    resultRow[RitDbTable.value],
                    resultRow[RitDbTable.value2]
                )
            }

            val body = DetailedRitdbBody(emptyMap(), rows)

            measureNanoTime {
                val nanoStart = System.nanoTime()
                val bytesOutput = cbor.writeValueAsBytes(body)
                val nanoEnd = System.nanoTime()
                serializationTime += (nanoEnd - nanoStart).nanoseconds

                bodySizes += bytesOutput.size

                val message = MqttMessage(bytesOutput)
                message.qos = 1
                mqttClient.publish("ritdb", message)
            }.let { significantTime += it.nanoseconds; callCounts++ }
        }

        val endInstant = Instant.now().also { println("End time: $it") }
        println("Total time: ${endInstant.epochSecond - startInstant.epochSecond}s")
        println("Significant time (serialization + sending): $significantTime, including serialization of $serializationTime")
        println("Average time per call: ${significantTime / callCounts.toDouble()}, including serialization of ${serializationTime / callCounts.toDouble()} as got called $callCounts times")

        val prefixArray = arrayOf("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi")
        val averageBodySize = bodySizes.average()
        var sizeIndex = log10(averageBodySize).toInt() / 3
        println("Average body size: ${(averageBodySize * 100 / 10.0.pow(sizeIndex * 3)).toInt().toFloat() / 100} ${prefixArray[sizeIndex]}B")

        delay(1000)
    }
}
