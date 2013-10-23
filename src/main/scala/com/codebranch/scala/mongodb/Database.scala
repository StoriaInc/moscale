package com.codebranch.scala.mongodb

import com.{mongodb => jmdb}


/**
 * MongoClient database wrapper.
 */
class Database(db: jmdb.DB) {

	def getCollection(name: String) = {
		val coll = db.getCollection(name)
		new Collection(coll)
	}

  def drop() {
    db.dropDatabase()
  }

}
