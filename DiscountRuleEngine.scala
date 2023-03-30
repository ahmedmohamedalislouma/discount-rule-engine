
import java.nio.file.{FileSystems, StandardWatchEventKinds}
import scala.collection.JavaConverters._
import java.nio.file.Paths
import java.nio.file.Path
import java.nio.file.Files
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import com.opencsv.{CSVParserBuilder, CSVReaderBuilder, CSVWriterBuilder, ICSVWriter}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.logging._

object RuleEngine extends App {
  // Connect to the MySQL database
  val url = "jdbc:mysql://localhost:3306/tablename"
  val username = "your username"
  val password = "your password"
  Class.forName("com.mysql.jdbc.Driver")
  val conn = DriverManager.getConnection(url, username, password)

  val dirToWatch = Paths.get("raw_data") // please add the full path of your dirctory
  

  val watchService = FileSystems.getDefault.newWatchService()
  dirToWatch.register(watchService, StandardWatchEventKinds.ENTRY_CREATE)

  var lines = List[List[String]]()
  var isFirstFile = true


  // Create a logger and a FileHandler to write the log messages to a file
  val logger = Logger.getLogger("RuleEngineLogger")
  val handler = new FileHandler("rule_engine.log", true) // please add the full path of your log file
  handler.setFormatter(new SimpleFormatter() {
    override def format(record: LogRecord): String = {
      val timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
      s"$timestamp ${record.getLevel.getName} ${record.getMessage}\n"
    }
  })


  logger.addHandler(handler)


  while (true) {
    val key = watchService.take()

    val newFiles = key.pollEvents().asScala.filter(event => event.kind() == StandardWatchEventKinds.ENTRY_CREATE).map(event => event.context().asInstanceOf[Path]).toList

    newFiles.foreach(file => {
      val fileToRead = dirToWatch.resolve(file)
      val fileName = file.getFileName().toString()
      // Use OpenCSV to read the CSV file
      val parser = new CSVParserBuilder().withSeparator(',').build()
      val reader = Files.newBufferedReader(fileToRead)
      val csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).build()
      val fileLines = csvReader.readAll().asScala.toList.map(_.toList).tail


      if (isFirstFile) {
        lines = fileLines
        isFirstFile = false
      } else {
        lines = lines ++ fileLines
      }

      val numRows = fileLines.size
      logger.log(Level.INFO, s"Received file $fileName with $numRows rows and inserted in database successfully ")
      Files.delete(fileToRead)
    })


    val daysRemainingDiscount = Map((0 to 29).map(day => day -> ((30 - day) * 0.01)*100).reverse: _*)


    // Define a function to calculate the days remaining for expiry
    def calculateDaysRemaining(expiryDate: String, transactionTimestamp: String): Int = {
      val expiryDateTime = LocalDateTime.parse(s"$expiryDate T00:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd 'T'HH:mm:ss"))
      val transactionInstant = Instant.parse(transactionTimestamp)
      val transactionDateTime = LocalDateTime.ofInstant(transactionInstant, ZoneOffset.UTC)
      val daysRemaining = expiryDateTime.toLocalDate.toEpochDay - transactionDateTime.toLocalDate.toEpochDay
      daysRemaining.toInt
    }


    def Rule1(order: List[String]): Double = {
      val daysRemaining = calculateDaysRemaining(order(2), order(0))
      if (daysRemaining <= 29 && daysRemaining >= 0) {
        daysRemainingDiscount.getOrElse(daysRemaining, 0.0)
      } else {
        0.0
      }
    }

    def Rule2(order: List[String]): Double = {
      order(1) match {
        case name if name.startsWith("Cheese") => 10 // 10% discount for cheese products
        case name if name.contains("Wine") => 5 // 5% discount for wine products
        case _ => 0 // no discount for other products
      }
    }

    def Rule3(order: List[String]): Double = {
      val dateTime = LocalDateTime.parse(order(0), DateTimeFormatter.ISO_DATE_TIME)
      val month = dateTime.getMonthValue
      val day = dateTime.getDayOfMonth
      if (month == 3 && day == 23) 50
      else 0

    }

    def Rule4(order: List[String]): Double = {
      if (order(3) >= "6" && order(3) <= "9") {
        5
      } else if (order(3) >= "10" && order(3) <= "14") {
        7
      } else if (order(3) > "15") {
        10
      } else {
        0.0
      }
    }

    def Rule5(order: List[String]): Double = {
      if (order(5) == "App") {
        val multiple = (order(3).toInt / 5).ceil.toInt * 5
        multiple match {
          case x if x == order(3).toInt => x // input is already a multiple of step
          case _ => multiple // round up to the nearest multiple of step
        }
      } else {
        0.0
      }
    }

    def Rule6(order: List[String]): Double = {
      if (order(6) == "Visa") {
        5
      } else {
        0.0
      }
    }


    // Apply the new rule to each order and calculate the total discount
    val Discount1 = lines.map(Rule1)
    val Discount2 = lines.map(Rule2)
    val Discount3 = lines.map(Rule3)
    val Discount4 = lines.map(Rule4)
    val Discount5 = lines.map(Rule5)
    val Discount6 = lines.map(Rule6)



    def getTopTwoAverages(lists: List[List[Double]]): List[Double] = {
      val numLists = lists.length
      val numElements = lists.head.length

      (0 until numElements).toList.map { i =>
        val values = lists.map(_.lift(i).getOrElse(0.0)).filter(_ != 0.0)
        values match {
          case head :: Nil => head
          case head :: tail =>
            val topTwo = values.sorted.reverse.take(2)
            topTwo.sum / 2.0
          case Nil => 0.0
        }
      }
    }


    val lists = List(Discount1, Discount2, Discount3, Discount4, Discount5, Discount6)

    val topTwoAverages = getTopTwoAverages(lists)

    val output1 = lines.zip(topTwoAverages).map { case (list, elem) => list :+ elem }


    def finalPrice(order: List[Any]): Double = {
      val quantity = order(3).toString.toDouble
      val price = order(4).toString.toDouble
      val discount = order(7).toString.toDouble
      quantity * price * ((100 - discount)/100)
    }

    val priceFinal = output1.map(finalPrice)

    val output = output1.zip(priceFinal).map { case (list, elem) => list :+ elem }
    val query = "INSERT INTO #tablename (timestamp, product, date, quantity, price, payment_method, payment_type, discount , final_price ) VALUES (?, ?, ?, ?, ?, ?, ?, ? , ?)"

    val pstmt: PreparedStatement = conn.prepareStatement(query)

    // Loop through the output and execute the PreparedStatement for each row
    output.foreach(row => {
      pstmt.setString(1, row(0).toString)
      pstmt.setString(2, row(1).toString)
      pstmt.setString(3, row(2).toString)
      pstmt.setInt(4, row(3).toString.toInt)
      pstmt.setDouble(5, row(4).toString.toDouble)
      pstmt.setString(6, row(5).toString)
      pstmt.setString(7, row(6).toString)
      pstmt.setDouble(8, row(7).toString.toDouble)
      pstmt.setDouble(9, row(8).toString.toDouble.round)
      pstmt.executeUpdate()
    })

    // Close the PreparedStatement and Connection
    // pstmt.close()
    // conn.close()

    key.reset()
  //
  }

}





