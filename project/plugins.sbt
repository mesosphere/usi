resolvers ++= Seq(
  Resolver.jcenterRepo,
  Resolver.sonatypeRepo("releases")
)
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.12")
addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.19.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.0")

// Documentation
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.3.2")
addSbtPlugin("io.github.jonas" % "sbt-paradox-material-theme" % "0.6.0")
