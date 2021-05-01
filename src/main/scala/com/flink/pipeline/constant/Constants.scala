package com.flink.pipeline.constant

object Constants {

  val ENV_PROFILE_URL = "PROFILE_URL"
  val ENV_SCHEMA_REGISTRY_URL = "SCHEMA_REGISTRY_URL"
  val ENV_KAFKA_URL = "KAFKA_URL"
  val ENV_ZOOKEEPER_URL = "ZOOKEEPER_URL"
  val DEFAULT_PROFILE_CONF_URL = "classpath:/application.conf"
  val DEFAULT_PROFILE_YAML_URL = "classpath:/application.yaml"
  val DEFAULT_RULE_DEFINITION_URL = "classpath:/pipeline_rules.json"

  val CONST_JSON = "json"
  val CONST_CSV = "csv"
  val CONST_AVRO = "avro"
  val CONST_KAFKA = "kafka"
  val CONST_FILE = "file"
  val CONST_PATH = "path"
  val CONST_TOPIC = "topic"
  val CONST_LOG_TYPE = "logType"
  val CONST_TYPE = "type"
  val CONST_FORMAT = "format"
  val CONST_SCHEMA = "schema"
  val CONST_SOURCE = "source"
  val CONST_SINKS = "sinks"
  val CONST_NAME = "name"
  val CONST_TRIGGER = "trigger"
  val CONST_EVENT_TIME_ATTR = "eventTimeAttribute"
  val CONST_CONSUMER = "consumer"
  val CONST_PRODUCER = "producer"
  val CONST_DATABASE = "database"
  val CONST_RULE_NAME = "rule_name"
  val CONST_PROPERTIES = "properties"
  val CONST_EVENT_TYPE = "eventType"
  val CONST_CORRELATION = "Correlation"
  val CONST_SCHEMA_REGISTRY = "schemaregistry"
  val CONST_PIPELINE_RULE = "pipelinerule"
  val CONST_HTTP = "http"
  val CONST_URL = "url"
  val CONST_ISROWTIME = "isRowtime"
  val CONST_WINDOW = "window"
  val CONST_WATERMARK = "watermark"
  val CONST_CLASSPATH = "classpath"
  val CONST_HASHCODE = "hash_code"

  val JFIELD_SCHEMA_VERSION = "version"
  val JFIELD_TOPIC = "topic"
  val JFIELD_NAME = "name"
  val JFIELD_SOURCE = "source"
  val JFIELD_SINKS = "sinks"
  val JFIELD_TRIGGER = "trigger"
  val JFIELD_RULE_NAME = "ruleName"
  val JFIELD_PROPERTIES = "properties"
  val JFIELD_EVENT_TYPE = "eventType"
  val JFIELD_ISROWTIME = "isRowtime"
  val JFIELD_HASH_CODE = "hashCode"
  val JFIELD_VERSION = "version"


  val DEFAULT_KAFKA_VERSION = "universal"
  val DEFAULT_EVENT_PACKAGE = "com.flink.pipeline.model"

  val COMMA = ','

  val KAFKA_GROUP_ID_PREFIX = "group.id"

  val CONFIG_PREFIX = "com.flink.pipeline.analytics"
  val KAFKA_CONSUMER = s"${CONFIG_PREFIX}.${CONST_KAFKA}.${CONST_CONSUMER}"
  val KAFKA_PRODUCER = s"${CONFIG_PREFIX}.${CONST_KAFKA}.${CONST_PRODUCER}"
  val DATABASE = s"${CONFIG_PREFIX}.${CONST_DATABASE}"
  val SCHEMA_REGISTRY = s"${CONFIG_PREFIX}.${CONST_SCHEMA_REGISTRY}"
  val PIPELINE_RULE = s"${CONFIG_PREFIX}.${CONST_PIPELINE_RULE}"

}
