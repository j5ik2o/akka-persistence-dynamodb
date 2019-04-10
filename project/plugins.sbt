resolvers ++= Seq(
  "Sonatype OSS Snapshot Repository" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype OSS Release Repository" at "https://oss.sonatype.org/content/repositories/releases/",
  "Seasar Repository" at "http://maven.seasar.org/maven2/",
  "Flyway" at "https://davidmweber.github.io/flyway-sbt.repo",
  Resolver.bintrayRepo("kamon-io", "sbt-plugins")
)

libraryDependencies ++= Seq(
  "com.h2database"  % "h2"         % "1.4.195",
  "commons-io"      % "commons-io" % "2.5",
  "org.seasar.util" % "s2util"     % "0.0.1"
)

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.0")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.1.0-M13")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.7")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")

addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.3.4")
