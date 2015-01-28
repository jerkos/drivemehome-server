package controllers

import akka.actor.Props
import models._

import play.api._
import play.api.libs.concurrent.Akka
import play.api.libs.iteratee.{Concurrent, Enumerator, Iteratee}
import play.api.mvc._
import play.api.libs.json._
import play.api.Play.current

import scala.concurrent.Future

// Reactive Mongo plugin, including the JSON-specialized collection
import play.modules.reactivemongo.MongoController
import play.modules.reactivemongo.json.collection.JSONCollection

import reactivemongo.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import akka.pattern.ask


object Application extends Controller with MongoController {


  def userCollection = db.collection[JSONCollection]("users")
  def pathCollection = db.collection[JSONCollection]("path")

  // ugly have to make several channels
  val (out, channel) = Concurrent.broadcast[JsValue]


  def insertTest() = {
    userCollection.insert(
      Json.obj(
        "name" -> "Marco",
        "email" -> "cram@hotmail.fr"
      )
    )
  }

  def insertPath(polyline: String) = {
    Logger.debug("inserting path")
    //val cursor: Cursor[JsObject] = null
    val cursor = userCollection.find(Json.obj("name"->"Marco")).cursor[JsObject]

    cursor.collect[List]().map { users =>

      if (users.isEmpty) {
        Logger.debug("insert marco user")
        insertTest()
      }
      val u = users(0)
      println(u)
      val driverM = ( u \ "_id")
      pathCollection.insert(
        Json.obj(
          "overview_polyline" -> polyline,
          "driver" -> driverM
        )
      )
    }
  }


  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  def login = Action { request =>
    var username = ""
    var password = ""
    request.body.asJson.map { json =>
      username = (json \ "username").as[String]
      password = (json \ "password").as[String]
      println(username)
      println(password)
    }

    Ok(Json.obj(
        "status_code"-> 200,
        "error_string"-> "",
        "response"->
          Json.obj(
            "login"-> username,
            "email"-> password,
            "has_car"-> false,
            "car"-> JsNull,
            "nb_as_driver"-> 0,
            "nb_as_pedestrian"-> 0,
            "achievements"-> JsNull
          )
      )
    )

  }

//  def onDriverConnection = WebSocket.using[JsValue] { request =>
//    Logger.debug("inside driver websocket")
//    val in = Iteratee.foreach[JsValue]{ json =>
//      val encodedPoly = (json \ "overview_polyline" \ "points").as[String]
//      insertPath(encodedPoly)
//      channel.push(Json.obj("response"->"hello boy"))
//    }
//    (in, out)
//  }

  // init room
  val room = Akka.system.actorOf(Props[Room])

  def onDriverConnection(username: String) = WebSocket.async[JsValue] { request =>
      Logger.debug("inside driver websocket")
      (room ? Driver(username))(5 seconds).mapTo[(Iteratee[JsValue, _], Enumerator[JsValue])]
  }

  def onWalkerConnection(username: String) = WebSocket.async[JsValue] { request =>
    Logger.debug("inside walker websocket")
      (room ? Walker(username))(5 seconds).mapTo[(Iteratee[JsValue, _], Enumerator[JsValue])]
  }
}