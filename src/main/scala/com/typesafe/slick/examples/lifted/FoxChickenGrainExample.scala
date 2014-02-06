package com.typesafe.slick.examples.lifted

// Use H2Driver to connect to an H2 database
import scala.slick.driver.H2Driver.simple._

/**
 * A simple example that uses statically typed queries against an in-memory
 * H2 database. The example data comes from Oracle's JDBC tutorial at
 * http://download.oracle.com/javase/tutorial/jdbc/basics/tables.html.
 */
object FoxChickenGrainExample extends App {

  // Case classes -------------------------------

  case class Fox(id: Int, name: String, chickenId: Option[Int])
  case class Chicken(id: Int, name: String, grainId: Option[Int])
  case class Grain(id: Int, name: String)

  case class ChickenGrain(chicken: Chicken, grain: Option[Grain])
  case class FoxChickenGrain(fox: Fox, chickenGrain: Option[ChickenGrain])

  // Tables -------------------------------------

  class FoxTable(tag: Tag) extends Table[Fox](tag, "FOX") {
    def id = column[Int]("ID")
    def name = column[String]("NAME")
    def chickenId = column[Option[Int]]("CHICKEN_ID")
    def * = (id, name, chickenId) <> (Fox.tupled, Fox.unapply)
    def ? = (id.?, name.?, chickenId).shaped <> (
      { row => row._1.map(_ => Fox(row._1.get, row._2.get, row._3)) },
      { (_: Any) => sys.error("Can't insert into optional projection")})
  }

  val FoxQuery = TableQuery[FoxTable]

  class ChickenTable(tag: Tag) extends Table[Chicken](tag, "CHICKEN") {
    def id = column[Int]("ID")
    def name = column[String]("NAME")
    def grainId = column[Option[Int]]("GRAIN_ID")
    def * = (id, name, grainId) <> (Chicken.tupled, Chicken.unapply)
    def ? = (id.?, name.?, grainId).shaped <> (
      { row => row._1.map(_ => Chicken(row._1.get, row._2.get, row._3)) },
      { (_: Any) => sys.error("Can't insert into optional projection")})
  }

  val ChickenQuery = TableQuery[ChickenTable]

  class GrainTable(tag: Tag) extends Table[Grain](tag, "GRAIN") {
    def id = column[Int]("ID")
    def name = column[String]("NAME")
    def * = (id, name) <> (Grain.tupled, Grain.unapply)
    def ? = (id.?, name.?).shaped <> (
      { row => row._1.map(_ => Grain(row._1.get, row._2.get)) },
      { (_: Any) => sys.error("Can't insert into optional projection")})
  }

  val GrainQuery = TableQuery[GrainTable]

  // Shapes -------------------------------------

  val foxChickenGrainShape = ???

  val chickenGrainShape = ???

  // Aggregates ---------------------------------

  val ChickenGrainQuery =
    ChickenQuery
      .leftJoin(GrainQuery)
      .on((chickenRow, grainRow) => chickenRow.grainId === grainRow.id)
      .map { case (chicken, grain) => (chicken, grain.?) }(chickenGrainShape)

  val FoxChickenGrainQuery =
    FoxQuery
      .leftJoin(ChickenGrainQuery)
      .on((foxRow, chickenGrainRow) => foxRow.chickenId === chickenGrainRow._1.id)
      .map { case (fox, (chicken, optGrain)) => (fox, chicken.?, optGrain) }(foxChickenGrainShape)

  // Application main ---------------------------

  Database.forURL("jdbc:h2:mem:test2", driver = "org.h2.Driver") withSession { implicit session =>

    (FoxQuery.ddl ++ ChickenQuery.ddl ++ GrainQuery.ddl).create

    FoxQuery ++= List(
      Fox(1, "Fox 1", Some(1)),
      Fox(2, "Fox 2", Some(2)),
      Fox(3, "Fox 3", None)
    )

    ChickenQuery ++= List(
      Chicken(1, "Chicken 1", Some(1)),
      Chicken(2, "Chicken 2", None)
    )

    GrainQuery ++= List(
      Grain(1, "Grain 1")
    )

    println()
    println("ChickenGrainQuery:")
    for(chickenGrain <- ChickenGrainQuery) {
      println(chickenGrain)
    }

    println()
    println("FoxChickenGrainQuery:")
    for(foxChickenGrain <- FoxChickenGrainQuery) {
      println(foxChickenGrain)
    }
  }
}
