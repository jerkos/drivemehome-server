name := "drivemehome-server"

version := "1.0"

lazy val `drivemehome-server` = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(jdbc ,
                            anorm ,
                            cache ,
                            ws,
                            "org.reactivemongo" %% "play2-reactivemongo" % "0.10.5.0.akka23")
                            //"com.google.maps" %% "google-maps-services" % "0.1.5")

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )  