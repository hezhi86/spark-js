import java.sql.{ Connection, DriverManager }

object Mysql {

    val mysqlConf = collection.mutable.Map(
        "driver" -> "com.mysql.jdbc.Driver",
        "url" -> "jdbc:mysql://10.100.163.73:3301/information_schema?autoReconnect=true",
        "username" -> "dev_user",
        "password" -> "isdevuser_666666"
    )

        /**
    * 创建mysql连接
    * @return
    */
    def getMysqlConn(): Connection = {
        Class.forName(mysqlConf("driver"))
        DriverManager.getConnection(mysqlConf("url"), mysqlConf("username"), mysqlConf("password"))
    }

    def updateRunningStatus(): Unit = {
        // create database connection
        val conn = getMysqlConn
        try {
        // create the statement, and run the select query
        val statement = conn.createStatement()
        val stschema  = conn.createStatement()
        val sql = "select table_name from information_schema.tables where TABLE_SCHEMA = 'paydb'"
        val resultSet = statement.executeQuery(sql)
        while (resultSet.next()) {
            val tableName = resultSet.getString("table_name")
            println(tableName+"==>")
            val sqlschema = "select column_name from information_schema.COLUMNS where table_name = '"+ tableName +"'"
            val rsschema  = stschema.executeQuery(sqlschema)
            while (rsschema.next()) {
                val column = rsschema.getString("column_name")
                print(column)
            }
            println()
          }
        } catch {
          case e:Throwable => e.printStackTrace()
        } finally {
          conn.close
        }
    }

    def main99(args: Array[String]): Unit = {
        println("====start!!====")

        updateRunningStatus


        println("====end!!====")
    }


}

