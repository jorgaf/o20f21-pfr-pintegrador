package joins
import kantan.csv._
import kantan.csv.generic._
import kantan.csv.ops._

import slick.jdbc.MySQLProfile.api._

import java.io.File
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.io.Codec

object JoinProvince extends App {
  //Representa a la fila
  case class Province(name: String, id: Long=0L)

  //Representa a la tabla
  class ProvinceTable(tag: Tag) extends Table[Province](tag, "PROVINCE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Province]
  }
  lazy val qryProvince = TableQuery[ProvinceTable]

  case class VehicleRegistation(modelo: String, tonelaje: Double, asientos: Int, provinceId: Long, id: Long=0L)

  class VehicleRegistrationTable(tag: Tag) extends Table[VehicleRegistation](tag, "VEHICLE_REGISTRATION") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def modelo = column[String]("MODELO")
    def tonelaje = column[Double]("TONELAJE")
    def asientos = column[Int]("ASIENTOS")
    def provinceId = column[Long]("ID_PROVINCE")

    def provinceRegistration = foreignKey("fk_province", provinceId, qryProvince)(_.id)

    def * = (modelo, tonelaje, asientos, provinceId, id).mapTo[VehicleRegistation]

  }
  lazy val registrationQry = TableQuery[VehicleRegistrationTable]

  val db = Database.forConfig("test01")
  def exec[T](program:DBIO[T]): T = Await.result(db.run(program), 2.seconds)

  println((qryProvince.schema ++ registrationQry.schema).createStatements.mkString)
  //exec(registrationQry.schema.create)

  //DISEÑAR E IMPLEMENTAR UN MECANISMO PARA AGREGAR FILAS A LA TABLA VEHICLE_REGISTRATION
  implicit val codec:Codec = Codec.ISO8859
  val path2DataFile = "/Users/jorgaf/Documents/Clases/Oct2020-Feb2021/Presencial/ProgramacionFuncional/DataProyecto/datos2008.csv"
  case class Matricula(
                        provincia: String,
                        clase: String,
                        combustible: String,
                        marca: String,
                        servicio: String,
                        modelo: String,
                        tonelaje: Double,
                        asientos: Int,
                        estratone: String,
                        estrasientos: String)

  val dataSource = new File(path2DataFile).readCsv[List, Matricula](rfc.withHeader)
  val rows = dataSource.filter(row => row.isRight)
  val values = rows.collect({ case Right(matricula) => matricula })

  val registration = values.map(row => (row.modelo, row.tonelaje, row.asientos, row.provincia))
  registration.take(20).foreach(println)

  def qryProvinceIdByName(proName: Rep[String]) = for {
    province <- qryProvince
    if province.name === proName
  }  yield province

  println(exec(qryProvinceIdByName("LOJA").result).head.id)

  def getSeqToInsert() =
    registration.take(10).map(t => VehicleRegistation(t._1, t._2, t._3, exec(qryProvinceIdByName(t._4).result).head.id))

  getSeqToInsert().foreach(println)

}
