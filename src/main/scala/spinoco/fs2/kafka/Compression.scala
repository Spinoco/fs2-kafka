package spinoco.fs2.kafka

/**
  * Currently driver supports only GZip and Snappy compression.
  */
object Compression  extends Enumeration {
  val GZip, Snappy = Value
}
