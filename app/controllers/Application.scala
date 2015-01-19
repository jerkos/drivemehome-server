package controllers

import play.api._
import play.api.libs.iteratee.{Concurrent, Enumerator, Iteratee}
import play.api.libs.json.{Json, JsNull}
import play.api.mvc._


import concurrent.ExecutionContext.Implicits.global


object Application extends Controller {

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

  def wsOpened = WebSocket.using[String] { request =>
      val in: Iteratee[String, Unit] = Iteratee.foreach[String](println(_))
      val out = Enumerator[String]("Hello")
      (in, out)

  }
}