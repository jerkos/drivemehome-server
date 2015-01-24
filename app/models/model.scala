package models

//import java.util.Date

import akka.actor.Actor
//import akka.actor.Actor.Receive
import org.joda.time.DateTime
import play.api.libs.iteratee.Concurrent.Channel
import play.api.libs.iteratee.{Iteratee, Concurrent, Enumerator}
import play.api.libs.json.{Json, JsValue}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.collection.mutable
//import scala.collection.mutable.HashMap

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


class Room extends Actor {
  val drivers = new mutable.HashMap[String, (Enumerator[JsValue], Channel[JsValue])]
  val walkers = new mutable.HashMap[String, (Enumerator[JsValue], Channel[JsValue])]

  val (commonEnumerator, commonChannel) = Concurrent.broadcast[JsValue]

  override def receive = {
    case Driver(username) => {
      drivers(username) =  Concurrent.broadcast[JsValue]

      val iteratee = Iteratee.foreach[JsValue](msg => {
        // TODO insert polyline in the database
        println("got a message:" + msg.toString())
        //commonChannel.push(
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
      }}

      sender ! (iteratee, drivers(username)._1) //commonEnumerator)
    }

    case Walker(username) => {

      walkers(username) =  Concurrent.broadcast[JsValue]

      val iteratee = Iteratee.foreach[JsValue](msg => {
        // TODO check to which driver send the data

        broadcastToAll(
          Json.obj(
            "walker" -> username,
            "connection_time" -> new DateTime().toString) // how to serialize date
        )

        // seems not necessary to send another message
        broadcastToDrivers(msg)

      }).map{ _ => {
        //user closed his websocket client
        walkers(username)._2.eofAndEnd()
        walkers -= username
      }}

      sender ! (iteratee, walkers(username)._1)
    }

  } // end receive

  def broadcastToDrivers(data: JsValue) = {
    drivers.foreach{ case (username, channels) =>
        channels._2.push(data)
    }
  }

  def broadcastToAll(data: JsValue) = {
    val all = drivers ++ walkers
    all.foreach{ case (surname, channels) =>
        channels._2.push(data);
    }
  }

}


object Driver