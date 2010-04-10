import sbt._

class CascalProject(info:ProjectInfo) extends DefaultProject(info) {
  val shorrockin = "Shorrockin Repository" at "http://maven.shorrockin.com"
}