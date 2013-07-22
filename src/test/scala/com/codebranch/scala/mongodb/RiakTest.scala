package com.codebranch.scala.mongodb

import org.specs2.mutable.{BeforeAfter, Specification}
import com.mongodb.{WriteConcern, MongoClientURI}
import java.util.UUID
import com.frumatic.scala.riak._
import com.basho.riak.client.IRiakObject

class Country extends RiakEntity {

  import handlers._
  import Country.Field._

  val name = Field[String](Name)
}

object Country {
  object Field {
    val Name = "name"
  }

  def apply(name: String): Country = {
    val country = new Country
    country.name := name
    country
  }
}

class City extends RiakEntity {

  import handlers._
  import City.Field._

  val name = Field[String](Name)
  val population = Field[Int](Population)
  val group = Field[String](Group, Some("none"))
  val tags = Field[List[String]](Tags)
  val country = Field[Country](Country_)

  override def equals(that: Any) = that match {
    case that: this.type => this.id == that.id
    case _ => false
  }

//  validator("Name must be defined") {
//    name.isDefined
//  }

  binIndex("name") {
    name.get
  }

  override def buildLinks: Seq[(String, String, String)] = {
    Seq(("Country", country.get.id.get.toString, "default"))
  }

}

object City {
  object Field {
    val Name = "name"
    val Population = "population"
    val Group = "group"
    val Tags = "tags"
    val Country_ = "country"
  }

  def apply(name: String, population: Int, country: Country, tags: String*): City = {
    val city = new City
    city.name := name
    city.population := population
    city.tags := tags.toList
    city.country := country
    city
  }

  def apply(name: String): City = {
    val city = new City
    city.name := name
    city
  }

  def random: City = {
    City(UUID.randomUUID().toString)
  }
}


class RiakTest extends Specification with BeforeAfter {

  import handlers._
  import City.Field._

  implicit var riak: RiakClient = _

  val countryRussia = Country("Russia")
  val countryChina = Country("China")
  val cities = List(
    City("Moscow", 12, countryRussia, "capital", "of", "Russia"),
    City("Shanghai", 18, countryChina, "capital", "of", "China"),
    City("Ufa", 1, countryRussia, "yet", "another", "city")
  )
  var cityBucket: Bucket[City] = _
  var anotherCityBucket: Bucket[City] = _
  var countryBucket: Bucket[Country] = _
  var resolveSiblingsSize: Int = _

  class TestResolver[T <: RiakEntity] extends ChooseHeadResolver[T] {
    override def resolve(siblings: Seq[IRiakObject]): IRiakObject = {
      resolveSiblingsSize = siblings.size
      super.resolve(siblings)
    }
  }


  sequential

  def before {
    riak = new RiakClient("localhost")
    cityBucket = riak.getBucket[City](resolver = Some(new TestResolver[City]))
    cityBucket.updateProperties(allowSiblings = true, lastWriteWins = false)

    anotherCityBucket = new RiakClient("localhost").getBucket[City](resolver = Some(new TestResolver[City]))
    countryBucket = riak.getBucket[Country]()

    println(cities)
  }

  def after {
  }


  "Riak wrapper" should {
    "store and fetch entities" in {
      cities.foreach(cityBucket.store)
      cityBucket.fetch(cities(1).id.get).get.name.get must be equalTo "Shanghai"
      countryBucket.store(countryRussia)
      countryBucket.store(countryChina)
    }

    "fetch by 2i" in {
      cityBucket.fetchByIndex("name", "Moscow").toList.length must be equalTo 1
    }

    "fetch using mapReduce" in {
      val mrb = new MapReduceBuilder
      val result = cityBucket.fetchMapReduce(mrb.bucketKeyInputs("City").filter(StartsWith(cities(0).id.get.toString take 3)).withMapPhase[City](CommonPhase.GetValues))
      result.size must be equalTo 3
    }

    "fetch by 2i using mapReduce ranged input" in {
      val mrb = new MapReduceBuilder
      val result = cityBucket.fetchMapReduce(mrb.indexKeyInputs("City", "name_bin").filterRange("Moscow", "Shaurma").withMapPhase[City](CommonPhase.GetValues))
      (Set(result(0).name.get, result(1).name.get) & Set("Moscow", "Shanghai")).size must be equalTo 2
    }

    "fetch by id using mapReduce list input" in {
      val mrb = new MapReduceBuilder
      val result = cityBucket.fetchMapReduce(mrb.bucketKeyInputs("City").filterList(cities(0).id.get.toString, cities(2).id.get.toString).withMapPhase[City](CommonPhase.GetValues))
      (Set(result(0).name.get, result(1).name.get) & Set("Moscow", "Ufa")).size must be equalTo 2
    }

    "walk links" in {
      val mrb = new MapReduceBuilder
      val result = countryBucket.fetchMapReduce(mrb.bucketKeyInputs("City").withMapPhase[Country](CommonPhase.WalkLinks).withMapPhase[Country](CommonPhase.GetValues))
      result.size must be equalTo 3
    }

    "fetch vclock on store" in {
      cities.foreach(anotherCityBucket.store)
      getTotalSiblingsCount must be equalTo cities.size

      cities.foreach(cityBucket.store)
      getTotalSiblingsCount must be equalTo cities.size
    }

    "create siblings with storeUnsafe" in {
      cities.foreach(anotherCityBucket.storeUnsafe)
      getTotalSiblingsCount must be equalTo cities.size * 2

      cities.foreach(cityBucket.storeUnsafe)
      getTotalSiblingsCount must be equalTo cities.size * 3
    }

    "resolve conflicts on store" in {
      cities.foreach(anotherCityBucket.store)
      getTotalSiblingsCount must be equalTo cities.size
    }

    "cache vclock for fetched entities" in {
      var city0 = cityBucket.fetch(cities(0).id.get).get
      val anotherCity0 = anotherCityBucket.fetch(cities(0).id.get).get

      city0.name := "New" + city0.name.get
      cityBucket.storeUnsafe(city0)
      getCitySiblingsCount(city0) must be equalTo 1

      city0 = cityBucket.fetch(cities(0).id.get).get
      city0.name := "New" + city0.name.get
      cityBucket.storeUnsafe(city0)
      getCitySiblingsCount(city0) must be equalTo 1

      city0.name := "New" + city0.name.get
      cityBucket.storeUnsafe(city0)
      getCitySiblingsCount(city0) must be equalTo 2

      anotherCityBucket.storeUnsafe(anotherCity0)
      getCitySiblingsCount(city0) must be equalTo 3

      cityBucket.storeUnsafe(city0)
      getCitySiblingsCount(city0) must be equalTo 4
    }

    "benchmark" in {
      var city0 = cities(0)
      var time = System.currentTimeMillis()
      for (i <- 1 to 300) {
        cityBucket.store(city0)
      }
      println("RIAK STORE TOTAL TIME: " + (System.currentTimeMillis() - time) / 300.0 + " ms")

      time = System.currentTimeMillis()
      for (i <- 1 to 300) {
        cityBucket.fetch(city0.id.get)
      }
      println("RIAK FETCH TOTAL TIME: " + (System.currentTimeMillis() - time) / 300.0 + " ms")
    }

    "cleanup" in {
      cities.foreach(cityBucket.delete)
      countryBucket.delete(countryRussia)
      countryBucket.delete(countryChina)
    }

    def getCitySiblingsCount(c: City): Int = {
      cityBucket fetch c.id.get
      resolveSiblingsSize
    }

    def getTotalSiblingsCount: Int = {
      var totalSiblings = 0
      cities.foreach(c => { cityBucket fetch c.id.get; totalSiblings += resolveSiblingsSize })
      totalSiblings
    }
  }

}
