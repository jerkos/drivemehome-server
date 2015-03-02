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

import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.concurrent.duration._

import akka.pattern.ask


object Application extends Controller with MongoController {

  val userCollection = db.collection[JSONCollection]("users")


  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }


  /**
   * LOGIN
   * @return future(response)
   */
  def login = Action.async { implicit request =>
    // TODO json validation
    request.body.asJson.map { json =>

      val username = (json \ "username").as[String]
      val password = (json \ "password").as[String]

      val f = userCollection.find(Json.obj("username"->username)).one[JsObject]
      f.map{ jsObjOpt =>
        if (jsObjOpt.isEmpty) {
          Logger.debug("can not find user")
          Ok(Json.obj(
            "status_code"-> 200,
            "error_string"-> "This login des not exist. Please register first.",
            "response" -> ""
          ))
        } else {
          val jsObj = jsObjOpt.get
          val databasePass = (jsObj \ "password").as[String]
          if (databasePass == password) {
            //access granted
            Ok(Json.obj("status_code" -> 200,
                        "error_string" -> "",
                        "response" -> jsObj)
            )
          } else {
            Ok(Json.obj(
              "status_code"-> 200,
              "error_string"-> "Password does not seem to be correct !",
              "response" -> ""
            ))
          }
        }

      }

    }.getOrElse {
      Future(BadRequest("Expecting Json data"))
    }

  } // end login

  /**
   * REGISTER function
   * @return future(response)
   */
  def register = Action.async { implicit request =>
    //TODO json validation
    request.body.asJson.map { json =>
      val username = (json \ "username").as[String]
      val fUser = userCollection.find(Json.obj("username" -> username)).one[JsObject]
      fUser.map { jsObj =>
        //Logger.debug("debug aleady exist:" +  jsObj.getOrElse("does not exist").toString)
        if (jsObj.isEmpty) {
          Logger.debug("Inserting a new user")
          userCollection.insert(json)
          Ok(Json.obj(
            "status_code" -> 200,
            "error_string" -> "",
            "response" -> json
          ))
        } else {
          Ok(Json.obj(
            "status_code" -> 200,
            "error_string" -> "Username already exists. Please choose another one.",
            "response" -> ""
          ))
        }
      }
    }.getOrElse {
      Future(BadRequest("Expecting Json data"))
    }
  }


  // init room
  val room = Akka.system.actorOf(Props[Room])

  def onDriverConnection(username: String) = WebSocket.async[JsValue] { request =>
    Logger.debug("inside driver websocket")
    (room ? Driver(username))(10 seconds).mapTo[(Iteratee[JsValue, _], Enumerator[JsValue])]
  }

  def onWalkerConnection(username: String) = WebSocket.async[JsValue] { request =>
    Logger.debug("inside walker websocket")
    (room ? Walker(username))(10 seconds).mapTo[(Iteratee[JsValue, _], Enumerator[JsValue])]
  }
}