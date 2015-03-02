package models

//import java.util.Date

import java.util.Date

import akka.actor.Actor
import controllers.Application._
import play.api.Logger
import play.modules.reactivemongo.MongoController
import play.modules.reactivemongo.json.collection._
import reactivemongo.api.indexes._
import reactivemongo.api.indexes.IndexType.{ Geo2DSpherical, Geo2D }

import scala.collection.JavaConverters._
import scala.util.Success
import scala.util.control.Breaks._
import scala.concurrent.duration.Duration

//import play.api._
//import play.api.mvc._
//import play.api.libs.concurrent.Execution.Implicits.defaultContext
//import play.api.libs.functional.syntax._
//import play.api.libs.json._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ Await, Future }
// Reactive Mongo imports
import reactivemongo.api._

//import akka.actor.Actor.Receive
import org.joda.time.DateTime
import play.api.libs.iteratee.Concurrent.Channel
import play.api.libs.iteratee.{ Iteratee, Concurrent, Enumerator }
import play.api.libs.json._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.collection.mutable
//import scala.collection.mutable.HashMap

import com.google.maps.model.{ LatLng, EncodedPolyline }

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

  def nbDrivers = drivers.size
  def nbWalkers = walkers.size

  override def receive = {
    case Driver(username) => {
      drivers(username) = Concurrent.broadcast[JsValue]

      val iteratee = Iteratee.foreach[JsValue](msg => {

        val message = (msg \ "message").as[String]
        message match {
          //message name could be changed
          case "register-path" =>

            Logger.debug(s"Driver ${username}: Got register-path message")
            // first idea, remove document
            // assuming the driver can loose his internet connection
            // it may not be good to remove directly the document
            // so, second idea is to check the done_cancelled flag. If not yet set,
            // do not remove else remove.
            // also, must be sure that nobody has the same username
            Driver.getDriverWithName(username).map { jsonOpt =>
              if (jsonOpt.isEmpty) {
                // no one yet in the database with this username, could safely insert it
                // and broadcast to others interested.
                Driver.newDriverInsertion(username, msg, broadcastToDriver)
              } else {
                //Someone already, check done_cancelled flag.
                val json = jsonOpt.get
                val isFinished = (json \ "done_or_cancelled").as[Boolean]
                if (isFinished) {
                  // previous was finished, so insert a new one
                  Driver.newDriverInsertion(username, msg, broadcastToDriver)
                } else {
                  // Welcome back, nothing to do here
                }
              }
            }
          // handle wanted disconnection/cancellation
          case "disconnect" =>
        }

        //        broadcastToAll(
        //          Json.obj(
        //            "driver" -> username,
        //            "connection_time" -> new DateTime().toString
        //          ) // how to serialize date
        //        )

      }).map { _ =>
        {
          //user closed his websocket client
          drivers(username)._2.eofAndEnd()
          drivers -= username

          // As explained above, it may not be good to remove the document
          // since we do not know if it is voluntary or not
          //Driver.removeConnectedDriver(username)

          // should create room ?
          //        broadcastToWalkers(
          //          Json.obj(
          //          "message" -> "driver-disconnection",
          //          "response" ->
          //            Json.obj(
          //              "username" -> username
          //            )
          //          )
          //        )

        }
      }

      sender ! (iteratee, drivers(username)._1) //commonEnumerator)
    }

    case Walker(username) => {

      walkers(username) = Concurrent.broadcast[JsValue]

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

            // broadcasting to the walker the  valid drivers
            Driver.broadcastValidDriversOnWalkerConnection(msg, broadcastToWalker)

          //val futurePaths = Driver.searchForDriversOnWalkerConnection(msg)
          //Driver.searchForDriversOnWalkerConnection(msg)
          //Driver.searchForWalkersOnDriverConnection()

          //            futurePaths.map { paths =>
          //              Logger.debug("# paths:" + paths.length)
          //              paths.map { path =>
          //                val driverName = (path \ "driver_username").as[String]  //Driver.getDriverName((path \ "driver_id").as[String])
          //                Logger.debug("Got one matching driver:" + driverName)
          //
          //                // broadcast to walker
          //                // send driver destination ?
          //                walkers(username)._2.push(Json.obj(
          //                  "message" -> "matching-drivers"
          //                )
          //                )
          //
          //                //broadcastToDriver(msg, driverName)
          //              }
          //            }
        } // end on-walker connection

        //        broadcastToAll(
        //          Json.obj(
        //            "kind" -> "walker",
        //            "username" -> username,
        //            "connection_time" -> new DateTime().toString,
        //            "tot_nb_drivers" -> nbDrivers,
        //            "tot_nb_walkers" -> nbWalkers
        //          ) // how to serialize date
        //        )

        // seems not necessary to send another message
        //broadcastToDrivers(msg)

      }).map { _ =>
        {
          //user closed his websocket client
          walkers(username)._2.eofAndEnd()
          walkers -= username

          // remove walker from connected walkers
          // build disconnected message
          // like driver do not remove directly that walker (could be a random disconnection)
          //Driver.removeConnectedWalker(username)
          broadcastToDrivers(
            Json.obj(
              "message" -> "walker-disconnection",
              "response" ->
                Json.obj(
                  "username" -> username)))

        }
      }

      sender ! (iteratee, walkers(username)._1)
    }

  } // end receive

  def broadcastToDrivers(data: JsValue) = {
    drivers.foreach {
      case (username, channels) =>
        channels._2.push(data)
    }
  }

  def broadcastToWalkers(data: JsValue) = {
    walkers.foreach {
      case (username, channels) =>
        channels._2.push(data)
    }
  }

  def broadcastToSomeDrivers(data: JsValue, driverNames: ArrayBuffer[String]) = {
    driverNames.foreach { username =>
      drivers(username)._2.push(data)
    }
  }

  def broadcastToAll(data: JsValue) = {
    val all = drivers ++ walkers
    all.foreach {
      case (surname, channels) =>
        channels._2.push(data);
    }
  }

  def broadcastToDriver(data: JsValue, username: String) = {
    Logger.debug("broadcasted to:" + username + ", message:" + data.toString())

    drivers(username)._2.push(data)
  }

  def broadcastToWalker(data: JsValue, username: String) = {
    Logger.debug(s"broadcasting to walker ${username} with message: ${data.toString}")
    walkers(username)._2.push(data)
  }

}

object Driver {

  val userCollection = db.collection[JSONCollection]("users")
  val driverCollection = db.collection[JSONCollection]("connectedDrivers")
  val walkerCollection = db.collection[JSONCollection]("connectedWalkers")

  import reactivemongo.api.indexes.Index

  // ensure driver index
  val created = driverCollection.indexesManager.ensure(
    new Index(
      List("bounds" -> Geo2DSpherical // path bounds bounding box ?
        //"points.loc" -> Geo2DSpherical) // all points (waypoints)
      )
    )
  )

  // ensure walker indexes on both destination and current locations
  walkerCollection.indexesManager.ensure(
    new Index(
      List(
        "current_location" -> Geo2DSpherical
        //"destination.destination_location" -> Geo2DSpherical
      )
    )
    )

  walkerCollection.indexesManager.ensure(
    new Index(
      List(
        "destination.destination_location" -> Geo2DSpherical
      )
    )
  )

  /**
   * Insert a newly connected driver into mongo
   * @param username
   * @param msg
   * @param callback
   */
  def newDriverInsertion(username: String, msg: JsValue, callback: (JsValue, String) => Unit) = {
    Logger.debug("Inserting path in mongodb")
    Driver.insertPath(msg)
    Logger.debug("Done")


    // try to find some matching walkers
    //FOR THE MOMENT ONLY WALKERS are doing this
    //val response = msg \ "response"
    //broadcast to concerned driver and walkers ?s
    //Driver.searchForWalkersOnDriverConnection(username, response, callback)
  }

  def insertPath(jsValue: JsValue) = {
    // assert that is the right message
    Logger.debug("response:" + jsValue.toString())
    require((jsValue \ "message").as[String] == "register-path")
    val response = jsValue \ "response"
    //val customer = response \ "driver" as[Customer]
    val driverName = response \ "driver_username"
    val routes = response \ "route"

    val southWest = routes \ "bounds" \ "southwest"
    val northEast = routes \ "bounds" \ "northeast"

    val (swLat, swLng) = ((southWest \ "lat").as[Double], (southWest \ "lng").as[Double])
    val (neLat, neLng) = ((northEast \ "lat").as[Double], (northEast \ "lng").as[Double])

    // create an index for all data points of the path may have to filter if the number of points is to
    // big
//    val encodedPath = new EncodedPolyline((routes \ "overview_polyline").as[String])
//    val latLngList = encodedPath.decodePath().asScala.toList
//    val arr = for (latLng <- latLngList) yield Json.obj("loc" -> Json.obj("type" -> "Point",
//      "coordinates" -> Json.arr(latLng.lng, latLng.lat)))
    //val pointsArray = Json.arr(**arr)

    driverCollection.insert(
      Json.obj(
        "driver_username" -> driverName, // could be its username
        "creation_date" -> new Date().getTime(),
        "overview_polyline" -> routes \ "overview_polyline",
        "bounds" ->
          Json.obj(
            "type" -> "Polygon",
            "coordinates" ->
              Json.arr(
                Json.arr(
                  Json.arr(swLng, swLat),
                  Json.arr(swLng, neLat),
                  Json.arr(neLng, neLat),
                  Json.arr(neLng, swLat),
                  Json.arr(swLng, swLat)) //routes \ "bounds",
                  )),
        //"points" -> arr, //pointsArray,
        "duration" -> routes \ "duration",
        //"duration_in_traffic" -> routes \ "duration_in_traffic",
        "end_address"-> routes \ "end_address",
        "end_location"-> routes \ "end_location",
        "done_or_cancelled" -> false // valid ?
        ))
  }

  def getDriverWithName(username: String): Future[Option[JsObject]] = {
    //Json.obj("_id" -> Json.obj("$oid" ->id))
    val query = Json.obj("username" -> username)
    val futureJsObjectOpt = driverCollection.find(query).one[JsObject]
    futureJsObjectOpt
  }

  def insertWalker(jsValue: JsValue) = {
    val response = (jsValue \ "response").as[JsObject]
    // insert all the response building 2DSpherical indexes on both
    // departure location and arrival location (Geojson Point)
    walkerCollection.insert(response) //, defaultContext)
  }

  /**
   * perform all request in a ASYNCHRONOUS way
   * @param latLngList
   * @return
   */
  def searchForWalkersNearFromDeparture(latLngList: List[LatLng]): List[Future[List[JsObject]]] = {
    latLngList.map { latLng =>
      val requestCurrLoc = Json.obj("current_location" ->
        Json.obj(
          "$near" ->
            Json.obj(
              "$geometry" ->
                Json.obj(
                  "type" -> "Point",
                  "coordinates" -> Json.arr(latLng.lng, latLng.lat)),
              "$maxDistance" -> 100)))
      val cursor = walkerCollection.find(requestCurrLoc).cursor[JsObject]
      cursor.collect[List]()
    }
  }

  /**
   * perform all request in a ASYNCHRONOUS way
   * @param latLngList
   * @return
   */
  def searchForWalkersNearFromArrival(latLngList: List[LatLng]):  List[Future[List[JsObject]]] = {
    latLngList.map { latLng =>
      val requestDestLoc = Json.obj("destination.destination_location" ->
        Json.obj(
          "$near" ->
            Json.obj(
              "$geometry" ->
                Json.obj(
                  "type" -> "Point",
                  "coordinates" -> Json.arr(latLng.lng, latLng.lat)),
              "$maxDistance" -> 100)))
      val cursor = walkerCollection.find(requestDestLoc).cursor[JsObject]
      cursor.collect[List]()
    }
  }

  // another way which is a BAD WAY
//  def searchForWalkersForDriverUsername(username: String, driverData: JsValue, callback: (JsValue, String) => Unit) {
//    // get the response object
//    val routes = driverData \ "routes"
//    val encodedPolyline = new EncodedPolyline((routes \ "overview_polyline" \ "points").as[String])
//    val latLngList = encodedPolyline.decodePath().asScala.toList
//
//    val fMatches = searchForWalkersNearFromArrival(latLngList) ++ searchForWalkersNearFromDeparture(latLngList)
//    fMatches.map { fListJsObj =>
//      fListJsObj.map { lJsObj =>
//        //cryptic ? is it really efficient
//        val selected = lJsObj.filter(jsObj => lJsObj.count(_ == jsObj) >= 2).distinct
//        selected.foreach { jsObj =>
//
//          val message = Json.obj(
//            "message" -> "on-walker-connection",
//            "response" -> jsObj)
//
//          callback(message, username)
//
//        }
//      } // end foreach
//    }
//  }

  /**
   * One of the main function on walker connection
   * all async
   */
  def broadcastValidDriversOnWalkerConnection(walkerMsg: JsValue, callback: (JsValue, String) => Unit) {
    val currWalkerUsername = (walkerMsg \ "response" \ "username").as[String]
    val currWalkerLoc = (walkerMsg \ "response" \ "current_location" \ "coordinates").as[List[Double]]

//    Logger.debug("currWalkerUsername: " + currWalkerUsername)
//    Logger.debug("currWalkerLoc: " + currWalkerLoc(0) + ", " + currWalkerLoc(1))

    // narrow driver candidates
    val driverListFt = searchNearDrivers(currWalkerLoc)

    //unravel the future
    val validDriversFt = driverListFt.flatMap{ driverList =>

//      if (driverList.isEmpty)
//        Logger.debug("narrowed driver list is empty")
//      else
//        Logger.debug("driver list in the place: " + driverList.length)

      val results = driverList.map { driver =>

        val driverUsername = (driver \ "driver_username").as[String]

        Logger.debug("driverUsername under investigation: " + driverUsername)

        val encodedPolyline = new EncodedPolyline((driver \ "overview_polyline").as[String])
        val latLngList = encodedPolyline.decodePath().asScala.toList

        Logger.debug("latLng list length :" + latLngList.length)

        val arrivalMatchesFt  = Future.sequence(searchForWalkersNearFromArrival(latLngList))
        val departureMatchesFt = Future.sequence(searchForWalkersNearFromDeparture(latLngList))

        val driverFt = arrivalMatchesFt.flatMap{ arrivalMatchesList =>
          val arrivalMatchesWalkers = arrivalMatchesList.flatMap(x => x).map(json => ( json \ "username").as[String]).toSet

          departureMatchesFt.map { departureMatchesList =>
            val departureMatchesWalkers = departureMatchesList.flatMap(x => x).map(json => (json \ "username").as[String]).toSet
            val intersection = arrivalMatchesWalkers.intersect(departureMatchesWalkers)

            if (intersection.contains(currWalkerUsername)) {
              Logger.debug("find matching driver: " + driverUsername)
              driver
            } else null
          }
        } // end driver
        driverFt
      }

      Future.sequence(results)

    } // end driversListFt.flatMap

    // finally broadcast the driver list to the client
    validDriversFt.map { driverList =>

      // filter null object
      val json = Json.obj("message" -> "get-valid-drivers",
                          "response" -> driverList.filter(_ != null)
      )
      //finally broadcast the message to the current walker
      callback(json, currWalkerUsername)
    }

  }


  /**
   * A way to get some driver when a walker is connecting to the service
   * This is blocking call, but may be faster because of the breakable block ?
   * @param walkerMsg
   * @return
   */
  def searchForDriversOnWalkerConnection(walkerMsg: JsValue) = {
    // Good Drivers should have an end-point close to the walker
    // one point of this route must be close enough from the walker current position
    // so must iterate on drivers in mongo

    val currWalkerUsername = (walkerMsg \ "username").as[String]
    val currWalkerLoc = (walkerMsg \ "response" \ "current_location" \ "coordinates").as[List[Double]]
    // get all the drivers list todo: improvement get only the ones which is near the walker position
    // driverCollection.find(Json.obj()).cursor[JsObject].collect[List]()
    val driverListFt = searchNearDrivers(currWalkerLoc)

    driverListFt.map { driverList =>
      driverList.map { driver =>
        val encodedPolyline = new EncodedPolyline((driver \ "overview_polyline").as[String])
        val latLngList = encodedPolyline.decodePath().asScala.toList

        breakable {
          // perform test for each variables
          // todo performance improvement take only 1/3 points

          latLngList.foreach { latLng =>

            // build the request for destination location
            val validWalkersFt = searchValidWalkers(latLng, field = "arrival")
            // sure to wait break otherwise, do not know what is the best
            val validWalkers = Await.result(validWalkersFt, Duration("10 seconds"))
            //converting to a set
            val validWalkersName = validWalkers.map(json => (json \ "username").as[String]).toSet

            if (validWalkersName.contains(currWalkerUsername)) {
              // pass to the second step
              // perform the other request
              var toBreak = false
              breakable {

                //breakable block
                latLngList.foreach { latLng =>

                  // build the request for destination location
                  val validWalkersFt = searchValidWalkers(latLng, field = "departure")

                  val validWalkers = Await.result(validWalkersFt, Duration("10 seconds"))
                  //converting to a set
                  val validWalkersName = validWalkers.map(json => (json \ "username").as[String]).toSet

                  if (validWalkersName.contains(currWalkerUsername)) {

                    // todo
                    // broadcast to that walker the current driver

                    toBreak = true
                    break
                  }
                }
              }
              // break if necessary
              if (toBreak) {
                break
              }
            }
          }
        }
      }
      null
    }
  }

  /**
   *
   * @param latLng
   * @param maxDistance
   * @param field
   * @return
   */
  def searchValidWalkers(latLng: LatLng, maxDistance: Int = 100, field: String = "departure"): Future[List[JsObject]] = {

    val fieldStr = if (field == "departure") "current_location" else "destination.destination_location"

    val requestDestLoc = Json.obj(fieldStr ->
      Json.obj(
        "$near" ->
          Json.obj(
            "$geometry" ->
              Json.obj(
                "type" -> "Point",
                "coordinates" -> Json.arr(latLng.lng, latLng.lat)),
            "$maxDistance" -> maxDistance)))

    val filter = Json.obj("username" -> 1, "_id" -> 0)

    val cursor = walkerCollection.find(requestDestLoc, filter).cursor[JsObject]
    cursor.collect[List]()
  }

  /**
   *
   * @param walkerLoc
   * @return
   */
  def searchNearDrivers(walkerLoc: List[Double], maxDistance: Int=100) = {
    val request = Json.obj("bounds" ->
      Json.obj(
        "$near" ->
          Json.obj(
            "$geometry" ->
              Json.obj(
                "type" -> "Point",
                "coordinates" -> Json.arr(walkerLoc(0), walkerLoc(1))),
            "$maxDistance" -> maxDistance)))

    val cursor = driverCollection.find(request).cursor[JsObject]
    cursor.collect[List]()
  }


  def removeConnectedWalker(username: String) = {
    val f = walkerCollection.remove(Json.obj("username" -> username))
  }

  def removeConnectedDriver(username: String) = {
    driverCollection.remove(Json.obj("username" -> username))
  }

}