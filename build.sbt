// https://typelevel.org/sbt-typelevel/faq.html#what-is-a-base-version-anyway
ThisBuild / tlBaseVersion := "0.1" // your current series x.y

ThisBuild / organization := "io.github.arturaz"
ThisBuild / organizationName := "Artūras Šlajus"
ThisBuild / startYear := Some(2025)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(
  // your GitHub handle and name
  tlGitHubDev("arturaz", "Artūras Šlajus")
)

// publish website from this branch
ThisBuild / tlSitePublishBranch := Some("main")

val Scala213 = "2.13.16"
ThisBuild / crossScalaVersions := Seq(Scala213, "3.3.7")
ThisBuild / scalaVersion := Scala213 // the default Scala

val Redis4CatsVersion = "2.0.1"
val Otel4sVersion = "0.14.0"

lazy val root = tlCrossRootProject.aggregate(core, effects, streams)

val MUnit = Seq(
  "org.scalameta" %% "munit" % "1.2.1" % Test,
  "org.typelevel" %% "munit-cats-effect" % "2.1.0" % Test
)

lazy val core = project
  .in(file("core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "otel4s-redis4cats-core",
    libraryDependencies ++= Seq(
      "dev.profunktor" %% "redis4cats-core" % Redis4CatsVersion,
      "org.typelevel" %% "otel4s-core-trace" % Otel4sVersion
    ) ++ MUnit,
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "dev.profunktor.redis4cats.otel4s.buildinfo",
    addCommandAlias(
      "prepareCi",
      "scalafmtAll;scalafmtSbt;scalafixAll;test;docs/tlSite;mimaReportBinaryIssues;githubWorkflowCheck"
    )
  )

lazy val effects = project
  .in(file("effects"))
  .dependsOn(core)
  .settings(
    name := "otel4s-redis4cats-effects",
    libraryDependencies ++= Seq(
      "dev.profunktor" %% "redis4cats-effects" % Redis4CatsVersion,
      "org.typelevel" %% "otel4s-core-trace" % Otel4sVersion
    )
  )

lazy val streams = project
  .in(file("streams"))
  .dependsOn(core)
  .settings(
    name := "otel4s-redis4cats-streams",
    libraryDependencies ++= Seq(
      "dev.profunktor" %% "redis4cats-streams" % Redis4CatsVersion,
      "org.typelevel" %% "otel4s-core-trace" % Otel4sVersion
    )
  )

lazy val docs = project.in(file("site")).enablePlugins(TypelevelSitePlugin)
