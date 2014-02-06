package com.typesafe.slick.examples.lifted

import scala.slick.driver.H2Driver.simple._
import scala.slick.ast.{ Node, Symbol }
import scala.slick.lifted.{ AbstractTable, ColumnBase, MappedProjection }

object FoxChickenGrainExample extends App {

  // Case classes -------------------------------

  // This example maps three database tables, `Fox`, `Chicken`, and `Grain`,
  // and models two left-join queries on the tables:
  //
  //   - `ChickenGrainQuery` is equivalent to `Chicken leftJoin Grain on ...`
  //   - `FoxChickenGrainQuery` is equivalent to `Fox leftJoin (Chicken  leftJoin Grain on ...) on ...`
  //
  // The aim is to allow the queries to be easily composed while also making
  // each query individually useful to the programmer:
  //
  //   `ChickenGrainQuery where (_.chicken.name === "Chicken Little") foreach { (record: ChickenGrain) => ... }`
  //
  // We're attempting to do this using a class called `TableAndMappedProjection` to implement a `?`
  // method on each table that allows us to select an option of it from the database while
  // still allowing things like `_.table.field` in join conditions.
  //
  // The solution we have works on plain queries, but throws a runtime exception
  // when using `_.table.field`. See the last lines of the file for the failing case.

  case class Fox(id: Int, name: String, chickenId: Option[Int])
  case class Chicken(id: Int, name: String, grainId: Option[Int])
  case class Grain(id: Int, name: String)

  case class ChickenGrain(chicken: Chicken, grain: Option[Grain])
  case class FoxChickenGrain(fox: Fox, chicken: Option[Chicken], grain: Option[Grain])

  // OptionTable --------------------------------

  // This is an attempt to create a data structure that behaves like a MappedProjection but
  // keeps a referenced to the original table to allow the developer to refer to the columns
  // involved by name.
  //
  // The problem with this approach appears to be that the columns in the table aren't
  // necessarily the same ones in the mapped projection. See the definition of `?` in any of
  // the tables below for an example.

  case class TableAndMappedProjection[Table <: AbstractTable[_], Unpacked, Packed](
    table: Table,
    projection: MappedProjection[Unpacked, Packed]) extends ColumnBase[Unpacked] {

    type Projection = MappedProjection[Unpacked, Packed]

    type Self = TableAndMappedProjection[_, _, _]
    override def toString = s"TableAndMappedProjection($table, $projection)"
    override def toNode: Node = projection.toNode
    def encodeRef(path: List[Symbol]) = TableAndMappedProjection(table.encodeRef(path), projection.encodeRef(path))
  }

  // Tables -------------------------------------

  // We define the tables using `TableAndMappedProjection` to provide a `?` method:

  class FoxTable(tag: Tag) extends Table[Fox](tag, "FOX") {
    def id = column[Int]("ID")
    def name = column[String]("NAME")
    def chickenId = column[Option[Int]]("CHICKEN_ID")
    def * = (id, name, chickenId) <> (Fox.tupled, Fox.unapply)
    def ? = TableAndMappedProjection(
      this,
      (id.?, name.?, chickenId).shaped <> (
        { row => row._1.map(_ => Fox(row._1.get, row._2.get, row._3)) },
        { (_: Any) => sys.error("Can't insert into optional projection")}))
  }

  type OptionalFox = TableAndMappedProjection[FoxTable, Option[Fox], (Option[Int], Option[String], Option[Int])]

  val FoxQuery = TableQuery[FoxTable]

  class ChickenTable(tag: Tag) extends Table[Chicken](tag, "CHICKEN") {
    def id = column[Int]("ID")
    def name = column[String]("NAME")
    def grainId = column[Option[Int]]("GRAIN_ID")
    def * = (id, name, grainId) <> (Chicken.tupled, Chicken.unapply)
    def ? = TableAndMappedProjection(this, (id.?, name.?, grainId).shaped <> (
      { row => row._1.map(_ => Chicken(row._1.get, row._2.get, row._3)) },
      { (_: Any) => sys.error("Can't insert into optional projection")}))
  }

  type OptionalChicken = TableAndMappedProjection[ChickenTable, Option[Chicken], (Option[Int], Option[String], Option[Int])]

  val ChickenQuery = TableQuery[ChickenTable]

  class GrainTable(tag: Tag) extends Table[Grain](tag, "GRAIN") {
    def id = column[Int]("ID")
    def name = column[String]("NAME")
    def * = (id, name) <> (Grain.tupled, Grain.unapply)
    def ? = TableAndMappedProjection(
      this,
      (id.?, name.?).shaped <> (
        { row => row._1.map(_ => Grain(row._1.get, row._2.get)) },
        { (_: Any) => sys.error("Can't insert into optional projection")}))
  }

  type OptionalGrain = TableAndMappedProjection[GrainTable, Option[Grain], (Option[Int], Option[String])]

  val GrainQuery = TableQuery[GrainTable]

  // Aggregates ---------------------------------

  // We define aggregate queries involving the tables:

  lazy val ChickenGrainQuery =
    ChickenQuery
      .leftJoin(GrainQuery)
      .on((chickenRow, grainRow) => chickenRow.grainId === grainRow.id)
      .map { case (chicken, grain) => (chicken, grain.?) }(chickenGrainShape)

  lazy val FoxChickenGrainQuery =
    FoxQuery
      .leftJoin(ChickenGrainQuery)
      .on((foxRow, chickenGrainRow) => foxRow.chickenId === chickenGrainRow._1.id)
      .map { case (fox, (chicken, optGrain)) => (fox, chicken.?, optGrain) }(foxChickenGrainShape)

  // Shapes -------------------------------------

  // Finally, we define shapes that allow us to retrieve the `FoxChickenGrain`
  // and `ChickenGrain` case classes directly from the results of each query:

  final class FoxChickenGrainShape[Level <: ShapeLevel](val shapes: Seq[Shape[_, _, _, _]])
    extends MappedScalaProductShape[Level, Product, (FoxTable, OptionalChicken, OptionalGrain), FoxChickenGrain, (FoxTable, OptionalChicken, OptionalGrain)] {

    def buildValue(elems: IndexedSeq[Any]) =
      if(elems(0).isInstanceOf[FoxTable]) {
        (elems(0).asInstanceOf[FoxTable], elems(1).asInstanceOf[OptionalChicken], elems(2).asInstanceOf[OptionalGrain])
      } else {
        FoxChickenGrain(elems(0).asInstanceOf[Fox], elems(1).asInstanceOf[Option[Chicken]], elems(2).asInstanceOf[Option[Grain]])
      }
    def copy(shapes: Seq[Shape[_, _, _, _]]) = new FoxChickenGrainShape(shapes)
  }

  implicit def foxChickenGrainShape[Level <: ShapeLevel] = new FoxChickenGrainShape[Level](Seq(
    FoxQuery.unpackable.shape,
    ChickenQuery.unpackable.shape,
    GrainQuery.unpackable.shape))

  final class ChickenGrainShape[Level <: ShapeLevel](val shapes: Seq[Shape[_, _, _, _]])
    extends MappedScalaProductShape[Level, Product, (ChickenTable, OptionalGrain), ChickenGrain, (ChickenTable, OptionalGrain)] {

    def buildValue(elems: IndexedSeq[Any]) = {
      if(elems(0).isInstanceOf[ChickenTable]) {
        (elems(0).asInstanceOf[ChickenTable], elems(1).asInstanceOf[OptionalGrain])
      } else {
        ChickenGrain(elems(0).asInstanceOf[Chicken], elems(1).asInstanceOf[Option[Grain]])
      }
    }
    def copy(shapes: Seq[Shape[_, _, _, _]]) = new ChickenGrainShape(shapes)
  }

  implicit def chickenGrainShape[Level <: ShapeLevel] = new ChickenGrainShape[Level](Seq(
    ChickenQuery.unpackable.shape,
    GrainQuery.unpackable.shape))

  // Application main ---------------------------

  Database.forURL("jdbc:h2:mem:test2", driver = "org.h2.Driver") withSession { implicit session =>

    // Create test data -------------------------

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

    // Actual test code -------------------------

    // This works fine, showing that the queries produce results of the correct type:

    println()
    println("ChickenGrainQuery:")
    ChickenGrainQuery foreach (println)

    // This works fine, showing that the queries can be nested:

    println()
    println("FoxChickenGrainQuery:")
    FoxChickenGrainQuery foreach (println)

    // This fails because `grain.table.name` isn't actually selected as part of the query
    // (the optional column in `grain.projection` is being selected instead):

    println()
    println("Filtered ChickenGrainQuery:")
    ChickenGrainQuery where { case (chicken: ChickenTable, grain: OptionalGrain) =>
      grain.table.name === "Grain 1"
    } foreach (println)
  }
}
