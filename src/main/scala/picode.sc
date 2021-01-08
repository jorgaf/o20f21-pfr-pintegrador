import java.io.File

import kantan.csv._
import kantan.csv.ops._

import scala.io.Codec

implicit val codec:Codec = Codec.ISO8859

val path2DataFile = "/Users/jorgaf/Documents/Clases/Oct2020-Feb2021/Presencial/ProgramacionFuncional/DataProyecto/datos2008.csv"
val dataSource = new File(path2DataFile).readCsv[List, (String, String, String, String, String, String, Double, Int, String, String)](rfc.withHeader())
dataSource.take(15).foreach(println)
val rows = dataSource.filter(row => row.isRight)
println(rows.size)

val errors = dataSource.filter(row => row.isLeft)
printf("Errors: %d", errors.size)

val values = rows.collect({ case Right(t10) => t10 })
values.take(15).foreach(println)

val clasesV = values.map(_._2).distinct.sorted