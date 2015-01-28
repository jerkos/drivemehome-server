package models

//import java.util.Date

import java.util.Date

import akka.actor.Actor
import controllers.Application._
import play.api.Logger
import play.modules.reactivemongo.MongoController
import play.modules.reactivemongo.json.collection._
import reactivemongo.api.indexes._
import reactivemongo.api.indexes.IndexType.{Geo2DSpherical, Geo2D}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

//import play.api._
//import play.api.mvc._
//import play.api.libs.concurrent.Execution.Implicits.defaultContext
//import play.api.libs.functional.syntax._
//import play.api.libs.json._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
// Reactive Mongo imports
import reactivemongo.api._

//import akka.actor.Actor.Receive
import org.joda.time.DateTime
import play.api.libs.iteratee.Concurrent.Channel
import play.api.libs.iteratee.{Iteratee, Concurrent, Enumerator}
import play.api.libs.json._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.collection.mutable
//import scala.collection.mutable.HashMap

import com.google.maps.model.{LatLng, EncodedPolyline}

/**
 * Created by Marc on 23/01/2015.
 */

//Connection
case class Driver(username: String)
case class Walker(username: String)

//Disconnection
case class DriverDisconnected(username: String)
case class WalkerDisconnected(username: String)

//broadcast to all, not necessary ?
//case class BroadcastToAll(data: JsValue)

//walker broadcast to all drivers
case class WalkerBroadcastToDrivers(username: String)

//
case class WalkerBroadcastToDriver(walkerUsername: String, driverUsername: String)

//
case class DriverBroadcastToWalker(driverUsername: String, walkerUsername: String)

//disconnection

//case class LatLng(lat: Double, lng: Double)


class Room extends Actor {
  val drivers = new mutable.HashMap[String, (Enumerator[JsValue], Channel[JsValue])]
  val walkers = new mutable.HashMap[String, (Enumerator[JsValue], Channel[JsValue])]

  val (commonEnumerator, commonChannel) = Concurrent.broadcast[JsValue]

  override def receive = {
    case Driver(username) => {
      drivers(username) =  Concurrent.broadcast[JsValue]

      val iteratee = Iteratee.foreach[JsValue](msg => {
        // insert polyline in the database
        val message = (msg \ "message").as[String]
        message match {
            //message name could be changed
          case "register-path" =>
            Logger.debug("Inserting path in mongodb")
            Driver.insertPath(msg)
            Logger.debug("Done")

            // try to find some matching walkers
            val response = msg \ "response"
            Driver.searchForWalkersOnDriverConnection(username, response, broadcastToDriver)
        }

        broadcastToAll(
          Json.obj(
            "driver" -> username,
            "connection_time" -> new DateTime().toString
          ) // how to serialize date
        )
      }).map{ _ => {
        //user closed his websocket client
        drivers(username)._2.eofAndEnd()
        drivers -= username

        Driver.removeConnectedDriver(username)
        // should create room ?
        broadcastToWalkers(
          Json.obj(
          "message" -> "driver-disconnection",
          "response" ->
            Json.obj(
              "username" -> username
            )
          )
        )

      }}

      sender ! (iteratee, drivers(username)._1) //commonEnumerator)
    }

    case Walker(username) => {

      walkers(username) =  Concurrent.broadcast[JsValue]

      val iteratee = Iteratee.foreach[JsValue](msg => {
        // TODO check to which driver send the data
        Logger.debug("walker got a message server-side:" + msg.toString())

        val message = (msg \ "message").as[String]

        message match {
          case "on-walker-connection" =>

            // put this in a new function
            Logger.debug("Inserting walker in mongo db")
            Driver.insertWalker(msg)
            Logger.debug("Done")

            val futurePaths = Driver.searchForDriversOnWalkerConnection(msg)

            futurePaths.map { paths =>
              Logger.debug("# paths:" + paths.length)
              paths.map { path =>
                val driverName = Driver.getDriverName((path \ "driver_id").as[String])
                driverName.map { opt =>
                  opt.map { x =>
                    val driverUsername = (x \ "username").as[String]
                    Logger.debug("Got one matching driver:" + driverUsername)
                    broadcastToDriver(msg, driverUsername)
                  }
                }
              }
            }
        }

        //val driverNames = Driver.insertWalker(msg)
        //broadcastToSomeDrivers(msg, driverNames)

        broadcastToAll(
          Json.obj(
            "walker" -> username,
            "connection_time" -> new DateTime().toString
          ) // how to serialize date
        )

        // seems not necessary to send another message
        //broadcastToDrivers(msg)

      }).map{ _ => {
        //user closed his websocket client
        walkers(username)._2.eofAndEnd()
        walkers -= username

        // remove walker from connected walkers
        // build disconnected message
        Driver.removeConnectedWalker(username)
        broadcastToDrivers(
          Json.obj(
            "message" -> "walker-disconnection",
            "response" ->
              Json.obj(
                "username" -> username
              )
          )
        )


      }}

      sender ! (iteratee, walkers(username)._1)
    }

  } // end receive

  def broadcastToDrivers(data: JsValue) = {
    drivers.foreach{ case (username, channels) =>
        channels._2.push(data)
    }
  }

  def broadcastToWalkers(data: JsValue) = {
    walkers.foreach{ case (username, channels) =>
      channels._2.push(data)
    }
  }


  def broadcastToSomeDrivers(data: JsValue, driverNames: ArrayBuffer[String]) = {
    driverNames.foreach{ username =>
      drivers(username)._2.push(data)
    }
  }

  def broadcastToAll(data: JsValue) = {
    val all = drivers ++ walkers
    all.foreach{ case (surname, channels) =>
        channels._2.push(data);
    }
  }

  def broadcastToDriver(data:JsValue, username: String) = {
    Logger.debug("broadcasted to:" + username + ", message:" + data.toString())

    drivers(username)._2.push(data)
  }

}


object Driver {

  def userCollection = db.collection[JSONCollection]("users")
  def driverCollection = db.collection[JSONCollection]("connectedDrivers")
  def walkerCollection = db.collection[JSONCollection]("connectedWalkers")

  import reactivemongo.api.indexes.Index

  // ensure driver inde
  val created = driverCollection.indexesManager.ensure(
    new Index(
      List("bounds" -> Geo2DSpherical)
    )
  )

  // ensure walker indexes on both destination and current locations
  walkerCollection.indexesManager.ensure(
    new Index(
    List(
      "current_location" -> Geo2DSpherical,
      "destination.destination_location" -> Geo2DSpherical)
    )
  )


  //println("Created ? : " + Await.result(created, Duration(5).seconds)).toString)

  def insertPath(jsValue: JsValue) = {
    // assert that is the right message
    Logger.debug("response:" + jsValue.toString())
    require( (jsValue \ "message").as[String] == "register-path")
    val response = jsValue \ "response"
    //val customer = response \ "driver" as[Customer]
    val driverID = response \ "driver_id"
    val routes = response \ "routes"

    val southWest = routes \ "bounds" \ "southwest"
    val northEast = routes \ "bounds" \ "northeast"

    val (swLat, swLng) = ( (southWest \"lat").as[Double], (southWest \ "lng").as[Double])
    val (neLat, neLng) = ( (northEast \ "lat").as[Double], (northEast\ "lng").as[Double] )


    driverCollection.insert(
      Json.obj(
        "driver_id" -> driverID,
        "creation_date" -> new Date().getTime(),
        "overview_polyline" -> routes \ "overview_polyline" \ "points",
        "bounds" ->
          Json.obj(
            "type" -> "Polygon",
            "coordinates"->
              Json.arr(
                Json.arr(
                  Json.arr(swLng, swLat),
                  Json.arr(swLng, neLat),
                  Json.arr(neLng, neLat),
                  Json.arr(neLng, swLat),
                  Json.arr(swLng, swLat)
                ) //routes \ "bounds",
              )
          ),
        "done_or_cancelled" -> false // valid ?
      )
    )
  }

  def getDriverName(id: String): Future[Option[JsObject]] = {
    val query = Json.obj("_id" -> Json.obj("$oid" ->id))
    val futureJsObjectOpt = userCollection.find(query).one[JsObject]
//    futureJsObjectOpt.map{ jsObjectOpt =>
//      jsObjectOpt.map(_)
//    }
    futureJsObjectOpt
  }

  def insertWalker(jsValue: JsValue) = {
    val response = (jsValue \ "response").as[JsObject]
    // insert all the response building 2DSpherical indexes on both
    // departure location and arrival location (Geojson Point)
    walkerCollection.insert(response) //, defaultContext)
  }

  def searchForDriversOnWalkerConnection(jsValue: JsValue): Future[List[JsObject]] = {
    require((jsValue \ "message").as[String] == "on-walker-connection")

    val currentLocation = (jsValue \ "response" \ "current_location" \ "coordinates").as[List[Double]]
    val lat = currentLocation(1) //(currentLocation \ "lat").as[Double]
    val lng = currentLocation(0) //(currentLocation \ "lng").as[Double]

    Logger.debug(s"lat lng: ${lat}, ${lng}")

    val request = Json.obj("bounds"->
      Json.obj(
        "$near" ->
          Json.obj(
            "$geometry" ->
              Json.obj(
                "type" -> "Point",
                "coordinates" -> Json.arr(lng, lat)
              ),
            "$maxDistance" -> 50
          )
      )
    )

    Logger.debug("request:" + request.toString())
    val cursor = pathCollection.find(request).cursor[JsObject]
    cursor.collect[List]()
  }



  def searchForWalkersNearFromDeparture(latLngList: List[LatLng]): List[Future[List[JsObject]]] = {
    latLngList.map { latLng =>
      val requestCurrLoc = Json.obj("current_location" ->
        Json.obj(
          "$near" ->
            Json.obj(
              "$geometry" ->
                Json.obj(
                  "type" -> "Point",
                  "coordinates" -> Json.arr(latLng.lng, latLng.lat)
                ),
              "$maxDistance" -> 100
            )
        )
      )
      val cursor = walkerCollection.find(requestCurrLoc).cursor[JsObject]
      cursor.collect[List]()
    }
  }

  def searchForWalkersNearFromArrival(latLngList: List[LatLng]): List[Future[List[JsObject]]] = {
    latLngList.map{ latLng =>
      val requestDestLoc = Json.obj("destination.destination_location" ->
        Json.obj(
          "$near" ->
            Json.obj(
              "$geometry" ->
                Json.obj(
                  "type" -> "Point",
                  "coordinates" -> Json.arr(latLng.lng, latLng.lat)
                ),
              "$maxDistance" -> 100
            )
        )
      )
      val cursor = walkerCollection.find(requestDestLoc).cursor[JsObject]
      cursor.collect[List]()
    }
  }

  // another way
  def searchForWalkersOnDriverConnection(username: String, driverData: JsValue, callback: (JsValue, String) => Unit) {
    // get the response object
    val routes = driverData \ "routes"
    val encodedPolyline = new EncodedPolyline((routes \ "overview_polyline" \ "points").as[String])
    val latLngList = encodedPolyline.decodePath().asScala.toList

    val fMatches = searchForWalkersNearFromArrival(latLngList) ++ searchForWalkersNearFromDeparture(latLngList)
    val f = fMatches.map{ fListJsObj =>
      fListJsObj.map{ lJsObj =>
        //cryptic ?
        val selected = lJsObj.filter(jsObj => lJsObj.count(_ == jsObj) >= 2).distinct
        selected.foreach { jsObj =>

          val message = Json.obj(
            "message" -> "on-walker-connection",
            "response" -> jsObj
          )

          callback(message, username)

        } // end foreach
      }
    }
  }

  def removeConnectedWalker(username: String) = {
    val f = walkerCollection.remove(Json.obj("username" -> username))
  }

  def removeConnectedDriver(username: String) = {
    driverCollection.remove(Json.obj("username" -> username))
  }

}