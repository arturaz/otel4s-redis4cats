// https://typelevel.org/sbt-typelevel/faq.html#what-is-a-base-version-anyway
ThisBuild / tlBaseVersion := "0.1" // your current series x.y

ThisBuild / organization := "arturaz"
ThisBuild / organizationName := "arturaz"
ThisBuild / startYear := Some(2025)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(
  // your GitHub handle and name
  tlGitHubDev("arturaz", "Artūras Šlajus")
)

// publish to s01.oss.sonatype.org (set to true to publish to oss.sonatype.org instead)
ThisBuild / tlSonatypeUseLegacyHost := false

// publish website from this branch
ThisBuild / tlSitePublishBranch := Some("main")

val Scala213 = "2.13.16"
ThisBuild / crossScalaVersions := Seq(Scala213, "3.3.4")
ThisBuild / scalaVersion := Scala213 // the default Scala

val Redis4CatsVersion = "1.7.2"
val Otel4sVersion = "0.11.2"

lazy val root = tlCrossRootProject.aggregate(effects, streams)

lazy val effects = project.in(file("effects"))
  .settings(
    name := "otel4s-redis4cats-effects",
    libraryDependencies ++= Seq(
      "dev.profunktor" %% "redis4cats-effects" % Redis4CatsVersion,
      "org.typelevel" %% "otel4s-core-trace" % Otel4sVersion,
    )
  )

lazy val streams = project.in(file("streams"))
  .settings(
    name := "otel4s-redis4cats-streams",
    libraryDependencies ++= Seq(
      "dev.profunktor" %% "redis4cats-streams" % Redis4CatsVersion,
      "org.typelevel" %% "otel4s-core-trace" % Otel4sVersion,
    )
  )

lazy val docs = project.in(file("site")).enablePlugins(TypelevelSitePlugin)
