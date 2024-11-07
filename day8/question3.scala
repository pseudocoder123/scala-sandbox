import java.sql.{Connection, DriverManager, SQLException, Statement, ResultSet}

case class Candidate(sno: Int, name: String, city: String)

object DatabaseConnectivity{
    private val MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver"
    private val TABLE_NAME = "candidates"

    // These in general should be env variables
    private val DB_URL = "jdbc:mysql://scaladb.mysql.database.azure.com:3306/saketh_muthoju_scala"
    private val DB_USERNAME = "mysqladmin"
    private val DB_PASSWORD = "*************"

    implicit def tupleToCandidate(tuple: (Int, String, String)): Candidate = Candidate(tuple._1, tuple._2, tuple._3)

    val candidateData: Array[(Int, String, String)] = Array((1, "Alice", "New York"), (2, "Bob", "Los Angeles"),(3, "Charlie", "Chicago"),(4, "Diana", "Houston"),
    (5, "Eve", "Phoenix"),(6, "Frank", "Philadelphia"),(7, "Grace", "San Antonio"),(8, "Hank", "San Diego"),(9, "Ivy", "Dallas"),(10, "Jack", "San Jose"),
    (11, "Kathy", "Austin"),(12, "Leo", "Jacksonville"),(13, "Mona", "Fort Worth"),(14, "Nina", "Columbus"),(15, "Oscar", "Charlotte"),(16, "Paul", "San Francisco"),
    (17, "Quinn", "Indianapolis"),(18, "Rita", "Seattle"),(19, "Steve", "Denver"),(20, "Tina", "Washington"),(21, "Uma", "Boston"),(22, "Vince", "El Paso"),
    (23, "Wendy", "Detroit"),(24, "Xander", "Nashville"),(25, "Yara", "Portland"),(26, "Zane", "Oklahoma City"),(27, "Aiden", "Las Vegas"),(28, "Bella", "Louisville"),
    (29, "Caleb", "Baltimore"),(30, "Daisy", "Milwaukee"),(31, "Ethan", "Albuquerque"),(32, "Fiona", "Tucson"),(33, "George", "Fresno"),(34, "Hazel", "Mesa"),
    (35, "Ian", "Sacramento"),(36, "Jill", "Atlanta"),(37, "Kyle", "Kansas City"),(38, "Luna", "Colorado Springs"),(39, "Mason", "Miami"),(40, "Nora", "Raleigh"),
    (41, "Owen", "Omaha"),(42, "Piper", "Long Beach"),(43, "Quincy", "Virginia Beach"),(44, "Ruby", "Oakland"),(45, "Sam", "Minneapolis"),(46, "Tara", "Tulsa"),
    (47, "Ursula", "Arlington"),(48, "Victor", "New Orleans"),(49, "Wade", "Wichita"),(50, "Xena", "Cleveland"))

    // Create the table `candidates`
    def createCandidatesTable(statement: Statement) = {
        val query = s"""
        CREATE TABLE IF NOT EXISTS $TABLE_NAME (
            sno INT PRIMARY KEY,
            name VARCHAR(100),
            city VARCHAR(100)
        )
        """
        statement.execute(query)
        println("Successfully creates the `candidates` table")
    }

    // Insert the record in bulk into `candidates`
    def insertRecordsInBulk(statement: Statement) = {
        val candidates: Array[Candidate] = candidateData.map(ele => ele) // implicit conversion from array(tuple) to array(candidate)

        val valuesStr: String = candidateData.foldLeft("")((accumulator, candidate) => {
            if(!accumulator.isEmpty) accumulator + s", (${candidate.sno}, '${candidate.name}', '${candidate.city}')"
            else s"(${candidate.sno}, '${candidate.name}', '${candidate.city}')"
        })

        val query = s"""
        INSERT INTO $TABLE_NAME (sno, name, city)
        VALUES ${valuesStr}
        """

        statement.executeUpdate(query)
        println("Successfully inserted the bulk data.")
    }

    // Insert method: Has query to insert a row into table
    def insertMethod(statement: Statement, candidate: Candidate): Unit = {
        val query = s"""
        INSERT INTO $TABLE_NAME (sno, name, city)
        VALUES (${candidate.sno}, '${candidate.name}', '${candidate.city}')
        """

        statement.executeUpdate(query)
        println("Successfully inserted the data.")
    }
    
    // Insert the records sequentially into candidates using `insertMethod` function
    def insertRecords(statement: Statement): Unit = {
        candidateData.foreach(ele => {
            insertMethod(statement, ele)
        })
        println("All the records are inserted successfully")
    }

    // Query to result the number of records 
    def queryNumberOfRecords(statement: Statement): Int = {
        val query = s"SELECT COUNT(*) FROM $TABLE_NAME"
        val resultSet: ResultSet = statement.executeQuery(query)

        if(resultSet.next()) resultSet.getInt(1) else 0
    }

    @main def processTask(): Unit = {
        var connection: Connection = null
        var statement: Statement = null

        try{
            Class.forName(MYSQL_DRIVER_CLASS) // Load driver class

            // Establish connection using creadentials
            connection = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)
            println("Connection successful")

            statement = connection.createStatement() // Create a statement

            createCandidatesTable(statement)
            // insertRecordsInBulk(statement)  // Insert all records in bulk to reduce the number of database operations.
            // insertMethod(statement, Candidate(51, "Saketh", "Hyderabad"))  // `insertMethod` supports the Candidate instance
            // insertMethod(statement, (52, "Saketh Muthoju", "Warangal"))  // `insertMethod` supports the tuple
            insertRecords(statement)

            val records = queryNumberOfRecords(statement)

            assert(records == candidateData.size)
            println(s"Verified: $records records inserted successfully")

        }catch{
            case e : ClassNotFoundException =>
                println("Error loading jdbc driver class!!")
                e.printStackTrace()
            case e: SQLException =>
                println("Error Connecting to the database!!")
                e.printStackTrace()
        }finally {
            if (statement != null) statement.close()
            if (connection != null) connection.close()
        }
    }
}

// Run by providing the classpath [scala --classpath <classpath> <filename.scala>]