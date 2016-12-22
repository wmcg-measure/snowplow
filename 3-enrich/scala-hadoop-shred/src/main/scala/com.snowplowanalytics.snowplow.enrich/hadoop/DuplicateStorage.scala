package com.snowplowanalytics.snowplow.enrich.hadoop

// Scalaz
import scalaz._
import Scalaz._

// Scala
import scala.util.control.NonFatal
import scala.collection.JavaConverters._

// AWS
import awscala._
import awscala.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model.{ PutItemRequest, AttributeValue, ConditionalCheckFailedException }

// JSON
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.fromJsonNode
import com.fasterxml.jackson.databind.JsonNode

// Iglu
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ProcessingMessageMethods._
import com.snowplowanalytics.iglu.client.validation.ValidatableJsonMethods._

// SCE
import com.snowplowanalytics.snowplow.enrich.common.ValidatedMessage

/**
  * Common trait for duplicate storages, storing triple of event attributes,
  * allowing to identify duplicated events across batches.
  * Currently, single implementation is `DuplicateStorage.DynamoDbStorage`.
  * Mocks can extend mix-in this trait
  */
trait DuplicateStorage {

  /**
    * Try to store parts of the event into duplicates table
    *
    * @param eventId snowplow event id (UUID)
    * @param eventFingerprint enriched event's fingerprint
    * @param etlTstamp timestamp of enrichment job
    * @return true if event is successfully stored in table,
    *         false if condition is failed - both eventId and fingerprint are already in table
    */
  def put(eventId: String, eventFingerprint: String, etlTstamp: String): Boolean
}


/**
  * Companion object holding ADT for possible configurations and concrete implementations
  */
object DuplicateStorage {

  /**
    * ADT to hold all possible types for duplicate storage configurations
    */
  sealed trait DuplicateStorageConfig

  /**
    * Configuration required to use duplicate-storage in dynamodb
    * This instance supposed to correspond to `iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/1-0-*`
    * Class supposed to used only for extracting and carrying configuration.
    * For actual interaction `DuplicateStorage` should be used
    *
    * @param name arbitrary human-readable name for storage target
    * @param accessKeyId AWS access key id
    * @param secretAccessKey AWS secret access key
    * @param awsRegion AWS region
    * @param dynamodbTable AWS DynamoDB to store duplicates
    */
  case class DynamoDbConfig(name: String, accessKeyId: String, secretAccessKey: String, awsRegion: String, dynamodbTable: String) extends DuplicateStorageConfig

  object DynamoDbConfig {

    implicit val formats = org.json4s.DefaultFormats

    /**
      * Extract `DuplicateStorageConfig` from base64-encoded self-describing JSON,
      * just extracted from CLI arguments (therefore wrapped in `Validation[Option]`
      * Lifted version of `extractFromBase64`
      *
      * @param base64Config base64-encoded self-describing JSON with `amazon_dynamodb_config` instance
      * @param igluResolver Iglu-resolver to check that JSON instance in `base64` is valid
      * @return successful none if option wasn't passed,
      *         failure if JSON is invalid or not correspond to `DuplicateStorage`
      *         some successful instance of `DuplicateStorage` otherwise
      */
    def extract(base64Config: ValidatedMessage[Option[String]], igluResolver: ValidatedNel[Resolver]): ValidatedNel[Option[DynamoDbConfig]] = {
      val nestedValidation = (base64Config.toValidationNel |@| igluResolver) { (config: Option[String], resolver: Resolver) =>
        config match {
          case Some(encodedConfig) =>
            for { config <- extractFromBase64(encodedConfig, resolver) } yield config.some
          case None => none.successNel
        }
      }

      nestedValidation.flatMap(identity)
    }

    /**
      * Extract `DuplicateStorageConfig` from base64-encoded self-describing JSON.
      * Also checks that JSON is valid
      *
      * @param base64 base64-encoded self-describing JSON with `amazon_dynamodb_config` instance
      * @param resolver Iglu-resolver to check that JSON instance in `base64` is valid
      * @return successful none if option wasn't passed,
      *         failure if JSON is invalid or not correspond to `DuplicateStorage`
      *         some successful instance of `DuplicateStorage` otherwise
      */
    def extractFromBase64(base64: String, resolver: Resolver): ValidatedNel[DynamoDbConfig] = {
      ShredJobConfig.base64ToJsonNode(base64, ShredJobConfig.DuplicateStorageConfigArg)  // Decode
        .toValidationNel                                                                 // Fix container type
        .flatMap { node: JsonNode => node.validate(dataOnly = true)(resolver) }          // Validate against schema
        .map(fromJsonNode)                                                               // Transform to JValue
        .flatMap { json: JValue =>                                                       // Extract
          Validation.fromTryCatch(json.extract[DynamoDbConfig]).leftMap(e => toProcMsg(e.getMessage)).toValidationNel
        }
    }
  }

  /**
    * Initialize storage object from configuration.
    * This supposed to be a universal constructor for `DuplicateStorage`,
    * it can create local DynamoDB connection for testing purposes
    * using special values
    *
    * @param config all configuration required to instantiate storage
    * @return valid storage if no exceptions were thrown
    */
  def initStorage(config: DuplicateStorageConfig): Validated[DuplicateStorage] =
    config match {
      case DynamoDbConfig("local", "", "", "", tableName) =>
        val client = DynamoDbStorage.getLocalClient
        val table = DynamoDbStorage.getOrCreateTable(client, tableName)
        new DynamoDbStorage(client, table).success
      case DynamoDbConfig(name, accessKeyId, secretAccessKey, awsRegion, tableName) =>
        try {
          val client: DynamoDB = DynamoDB(accessKeyId, secretAccessKey)(Region.apply(awsRegion))
          val table = DynamoDbStorage.getOrCreateTable(client, tableName)
          new DynamoDbStorage(client, table).success
        } catch {
          case NonFatal(e) =>
            toProcMsg("Cannot initialize duplicate storage:\n" + Option(e.getMessage).getOrElse("")).failure
        }
    }

  /**
    * Primary duplicate storage object, supposed to handle interactions with DynamoDB table
    * These objects encapsulate DynamoDB client, which contains lot of mutable state,
    * references and unserializable objects - therefore it should be constructed on last step -
    * straight inside `ShredJob`. To initialize use `initStorage`
    *
    * @param client AWS DynamoDB client object
    * @param table DynamodDB table name with duplicates
    */
  class DynamoDbStorage private[hadoop](client: DynamoDB, table: String) extends DuplicateStorage {

    def put(eventId: String, eventFingerprint: String, etlTstamp: String): Boolean = {
      val putRequest = putRequestDummy
        .withExpressionAttributeValues(Map(":tst" -> new AttributeValue(etlTstamp)).asJava)
        .withItem(attributeValues(List("eventId" -> eventId, "fingerprint" -> eventFingerprint, "etlTime" -> etlTstamp)))

      try {
        client.putItem(putRequest)
        true
      } catch {
        case _: ConditionalCheckFailedException => false
      }
    }

    /**
      * Dummy request required to set all attributes and `tst` value attribute
      */
    val putRequestDummy: PutItemRequest = new PutItemRequest()
      .withTableName(table)
      .withConditionExpression(s"(attribute_not_exists(eventId) AND attribute_not_exists(fingerprint)) OR etlTime = :tst")

    /**
      * Helper method to transform list arguments into DynamoDB-compatible item with its
      * attributes
      *
      * @param attributes list of pairs of names and values, where values are string
      *                    and integers only
      * @return Java-compatible Hash-map
      */
    private def attributeValues(attributes: Seq[(String, Any)]): java.util.Map[String, AttributeValue] =
      attributes.toMap.mapValues(asAttributeValue).asJava

    /**
      * Convert **only** string and integer to DynamoDB-compatible object
      */
    private def asAttributeValue(v: Any): AttributeValue = {
      val value = new AttributeValue
      v match {
        case s: String => value.withS(s)
        case n: java.lang.Number => value.withN(n.toString)
        case _ => null
      }
    }
  }

  object DynamoDbStorage {

    /**
      * Indempotent action to create duplicates table
      * If table with name already exists - do nothing and just return name
      * If table doesn't exist - create table with predefined structure and return name
      *
      * @param client AWS DynamoDB client with established connection
      * @param name DynamoDB table name
      * @return same table name or throw exception
      */
    def getOrCreateTable(client: DynamoDB, name: String): String = {
      client.table(name) match {
        case Some(_) => name
        case None => createTable(client, name).name
      }
    }

    /**
      * Get DynamoDB connected to local service at 127.0.0.1:8000
      */
    private[hadoop] def getLocalClient = {
      val client = DynamoDB("", "")(Region.default())
      client.setEndpoint("http://127.0.0.1:8000")
      client
    }

    /**
      * Create DynamoDB table with indexes designed to store event duplicate information
      *
      * @param client AWS DynamoDB client with established connection
      * @param name DynamoDB table name
      * @return table object
      */
    private[hadoop] def createTable(client: DynamoDB, name: String) = {
      client.createTable(
        name,
        hashPK = "eventId" -> AttributeType.String,
        rangePK = "fingerprint" -> AttributeType.String,
        otherAttributes = Nil,
        indexes = Nil
      )
    }
  }
}
