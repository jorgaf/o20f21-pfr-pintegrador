package pi.loaderdata

import kantan.csv._
import kantan.csv.generic._
import kantan.csv.ops._


import java.io.File
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Codec

object UtilProvince {
  implicit val codec:Codec = Codec.ISO8859
  val path2DataFile = "<ruta_al_archivo>/datos2008.csv"

  //Representa a la fila


  //Representa a la tabla

  //Medio para la ejecución de consultas

  //Conexión con la base de datos


  def main(args: Array[String]): Unit = {
    //1. Crear esquema de datos, una única vez
    //createDataSchema()
    //2. Cargar datos desde el archivo
    //val provinceList = loadDataFromFile(path2DataFile)
    //3. Importar datos a la base de datos
    //importToDataBase(provinceList)
    //4. Realizar consultas
    provinceQueries()

  }

  def loadDataFromFile(path2DataFile: String): List[String] = {
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

    List("Aún", "Falta", "Código")
  }

  def createDataSchema(): Unit = {

  }

  def importToDataBase(pronviceList: List[String]): Unit = {


  }

  def provinceQueries(): Unit = {


  }

  //def exec[T](program:DBIO[T]): T = Await.result(db.run(program), 2.seconds)

}
