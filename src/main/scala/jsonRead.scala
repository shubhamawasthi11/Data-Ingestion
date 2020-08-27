//import libraries
import java.io.StringWriter
import PortalProject.Person
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule



object jsonRead {
  def main (args: Array[String]) {
    //create an object for json
    val person = Person("fred", 25)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    //convert object to string (serialize)
    val out = new StringWriter
    mapper.writeValue(out, person)
    val json = out.toString()
    println(json)

    //convert back to object (de-serialize)
    val person2 = mapper.readValue(json, classOf[Person])
    println(person2)
  }
}