package joins

import slick.jdbc.MySQLProfile.api._

object JoinProvince {
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


  }
  lazy val registrationQry = TableQuery[VehicleRegistrationTable]
}
