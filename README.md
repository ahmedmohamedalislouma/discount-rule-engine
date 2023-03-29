# discount-rule-engine
This is a Scala program that processes CSV files in a specified directory and applies a set of rules to the data to calculate discounts for each transaction. The discounted transactions are then inserted into a MySQL database table.

# Installation

Clone the repository from GitHub:

git clone https://github.com/ahmedmohamedalislouma/discount-rule-engine.git


# SBT Configuration
This project uses SBT (Scala Build Tool) as its build system. The build.sbt file is located at the root of the project directory and contains the following configuration:

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "your project name",
    libraryDependencies ++= Seq(
      "com.opencsv" % "opencsv" % "5.7.1",
      "mysql" % "mysql-connector-java" % "8.0.32"
    )
  )
  
  
The version and scalaVersion settings are used to specify the version of the project and the version of Scala to use, respectively.

The lazy val root setting is used to define the project itself. In this case, the project is defined as a single module with the name "your project name". The settings block contains various settings for the project, including library dependencies. In this case, the project depends on the OpenCSV and MySQL Connector/J libraries.

To build the project, run the sbt compile command. This will compile the project and download any necessary dependencies. To run the project, use the sbt run command.

# Usage

Place CSV files in the raw_data directory.
The program will automatically process any new CSV files that are added to the directory.
The discounted transactions will be inserted into the table in the MySQL database.

# Configuration

The MySQL database connection details are configured in the RuleEngine object. You can modify these details as needed.

val url = "jdbc:mysql://localhost:3306/tablename"
val username = "your username"
val password = "your password"
The directory to watch for new CSV files is also configured in the RuleEngine object. You can modify this directory as needed.

val dirToWatch = Paths.get("E:\\Scala\\Lab1\\src\\main\\scala\\raw_data")
The log file path is also configured in the RuleEngine object. You can modify this path as needed.

val handler = new FileHandler("E:\\Scala\\Lab1\\src\\main\\scala\\rule_engine.log", true)
