package dx.yaml

/**
 * Provides all the predefined YamlFormats.
 */
trait DefaultYamlProtocol
  extends BasicFormats
  with StandardFormats
  with CollectionFormats
  with ProductFormats
  with AdditionalFormats

object DefaultYamlProtocol extends DefaultYamlProtocol
