package com.frumatic.scala.riak

import com.basho.riak.client.http.{RiakConfig => HttpRiakConfig, RiakClient => HttpRiakClient}
import com.basho.riak.client.raw.RawClient
import com.basho.riak.client.raw.http.HTTPClientAdapter
import com.basho.riak.client.raw.pbc.{PBClientConfig, PBRiakClientFactory}

class RiakClient(httpClient: RawClient, pbClient: RawClient) {

  import com.codebranch.scala.mongodb.handlers.entityTypeHandler

  httpClient.generateAndSetClientId()
  pbClient.generateAndSetClientId()

  def this(host: String, httpPort: Int = 8098, pbPort: Int = 8087,
           timeout: IntOption = IntOption(Some(5000)), poolSize: IntOption = IntOption(Some(4))) =
    this(new HTTPClientAdapter(new HttpRiakClient(RiakHttpConfig(host, httpPort, timeout.value))),
      PBRiakClientFactory.getInstance().newClient(RiakPbConfig(host, pbPort, timeout.value, poolSize.value))
    )

  def getBucket[T <: RiakEntity](converter: Option[Converter[T]] = None,
                                 mutator: Option[Mutator[T]] = None,
                                 resolver: Option[Resolver[T]] = None)
                                (implicit m: Manifest[T]): Bucket[T] = {
    val annotation = Option(m.runtimeClass.getAnnotation(classOf[BucketSettings]))
    val bucketName = annotation.map(_.bucketName).getOrElse(m.runtimeClass.getSimpleName)
    new Bucket[T](bucketName, httpClient, pbClient,
      converter.getOrElse(new RiakEntityConverter[T]),
      mutator.getOrElse(new OverwriteMutator[T]),
      resolver.getOrElse(new ChooseHeadResolver[T]))
  }

}


object RiakHttpConfig {
  def apply(host: String, port: Int, timeout: Option[Int]): HttpRiakConfig = {
    val config = new HttpRiakConfig(host, port.toString, "/riak")
    timeout.foreach(t => config.setTimeout(t))
    config
  }
}

object RiakPbConfig {
  def apply(host: String, port: Int, timeout: Option[Int], poolSize: Option[Int]): PBClientConfig = {
    val configBuilder = PBClientConfig.Builder.from(PBClientConfig.defaults()).withHost(host).withPort(port)
    timeout.foreach(t => configBuilder.withConnectionTimeoutMillis(t).withRequestTimeoutMillis(t))
    poolSize.foreach(configBuilder.withPoolSize)
    configBuilder.build()
  }
}