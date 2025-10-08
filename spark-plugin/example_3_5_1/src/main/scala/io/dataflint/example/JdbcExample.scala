package io.dataflint.example

import org.apache.spark.sql.{SaveMode, SparkSession}
import java.sql.{Connection, DriverManager, Statement}

object JdbcExample extends App {
  // Initialize embedded H2 database
  val h2Url = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1"
  val h2User = "sa"
  val h2Password = ""
  
  println("=== Initializing H2 Database ===")
  setupH2Database()
  
  // Create Spark session with Dataflint plugin
  val spark = SparkSession
    .builder()
    .appName("JdbcExample")
    .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
    .config("spark.ui.port", "10000")
    .config("spark.dataflint.telemetry.enabled", value = false)
    .config("spark.eventLog.enabled", "true")
    .config("spark.sql.maxMetadataStringLength", "10000")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  try {
    println("\n=== Example 1: Simple Table Read ===")
    spark.sparkContext.setJobDescription("JDBC: Read all employees")
    val allEmployees = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    allEmployees.show()
    println(s"Total employees: ${allEmployees.count()}")

    println("\n=== Example 2: Read with Predicate Pushdown ===")
    spark.sparkContext.setJobDescription("JDBC: Read high-salary employees")
    val highSalaryEmployees = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
      .filter($"salary" > 75000)
    
    println("Employees with salary > 75000:")
    highSalaryEmployees.show()

    println("\n=== Example 3: Partitioned Parallel Read ===")
    spark.sparkContext.setJobDescription("JDBC: Partitioned read of employees")
    val partitionedRead = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .option("partitionColumn", "id")
      .option("lowerBound", "1")
      .option("upperBound", "100")
      .option("numPartitions", "4")
      .load()
    
    println(s"Number of partitions: ${partitionedRead.rdd.getNumPartitions}")
    partitionedRead.show()

    println("\n=== Example 4: Custom SQL Query ===")
    spark.sparkContext.setJobDescription("JDBC: Custom query with join")
    val customQuery = """
      (SELECT e.id, e.name, e.salary, d.dept_name, d.location
       FROM employees e
       JOIN departments d ON e.dept_id = d.dept_id
       WHERE e.salary > 60000
       ORDER BY e.salary DESC) AS custom_query
    """
    
    val joinedData = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", customQuery)
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    println("Employees with departments (salary > 60000):")
    joinedData.show()

    println("\n=== Example 5: Read Departments Table ===")
    spark.sparkContext.setJobDescription("JDBC: Read all departments")
    val departments = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "departments")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    departments.show()

    println("\n=== Example 6: Aggregation After JDBC Read ===")
    spark.sparkContext.setJobDescription("JDBC: Department salary aggregation")
    val deptSalaryAgg = allEmployees
      .join(departments, "dept_id")
      .groupBy("dept_name")
      .agg(
        org.apache.spark.sql.functions.count("id").as("employee_count"),
        org.apache.spark.sql.functions.avg("salary").as("avg_salary"),
        org.apache.spark.sql.functions.max("salary").as("max_salary")
      )
      .orderBy(org.apache.spark.sql.functions.desc("avg_salary"))
    
    println("Salary statistics by department:")
    deptSalaryAgg.show()

    println("\n=== Example 7: Write Back to JDBC ===")
    spark.sparkContext.setJobDescription("JDBC: Write aggregated data")
    deptSalaryAgg.write
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "dept_statistics")
      .option("user", h2User)
      .option("password", h2Password)
      .mode(SaveMode.Overwrite)
      .save()
    
    println("Successfully wrote department statistics back to database")
    
    // Read back the written data
    val writtenData = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "dept_statistics")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    println("Data written to dept_statistics table:")
    writtenData.show()

    println("\n=== Example 8: Read-Process-Write Pipeline ===")
    println("Pipeline: Read employees -> Calculate bonuses -> Write to new table")
    
    // Step 1: Read from JDBC
    spark.sparkContext.setJobDescription("JDBC Pipeline: Read employees for bonus calculation")
    val employeesForBonus = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    println("Step 1 - Employees read from database:")
    employeesForBonus.show(5)
    
    // Step 2: Process data - Calculate bonuses based on salary and tenure
    spark.sparkContext.setJobDescription("JDBC Pipeline: Process and calculate bonuses")
    val employeesWithBonus = employeesForBonus
      .withColumn("years_employed", 
        org.apache.spark.sql.functions.floor(
          org.apache.spark.sql.functions.datediff(
            org.apache.spark.sql.functions.current_date(),
            $"hire_date"
          ) / 365
        )
      )
      .withColumn("bonus_percentage",
        org.apache.spark.sql.functions.when($"years_employed" >= 5, 0.15)
          .when($"years_employed" >= 3, 0.10)
          .when($"years_employed" >= 1, 0.05)
          .otherwise(0.02)
      )
      .withColumn("bonus_amount", 
        org.apache.spark.sql.functions.round($"salary" * $"bonus_percentage", 2)
      )
      .withColumn("total_compensation",
        org.apache.spark.sql.functions.round($"salary" + $"bonus_amount", 2)
      )
      .select("id", "name", "dept_id", "salary", "hire_date", 
              "years_employed", "bonus_percentage", "bonus_amount", "total_compensation")
    
    println("Step 2 - Processed data with calculated bonuses:")
    employeesWithBonus.orderBy(org.apache.spark.sql.functions.desc("bonus_amount")).show(10)
    
    // Step 3: Write processed data back to JDBC
    spark.sparkContext.setJobDescription("JDBC Pipeline: Write bonus data to database")
    employeesWithBonus.write
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employee_bonuses")
      .option("user", h2User)
      .option("password", h2Password)
      .mode(SaveMode.Overwrite)
      .save()
    
    println("Step 3 - Successfully wrote employee bonus data to 'employee_bonuses' table")
    
    // Verify the written data
    val bonusData = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employee_bonuses")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    println("Verification - Data in employee_bonuses table:")
    bonusData.orderBy(org.apache.spark.sql.functions.desc("bonus_amount")).show(10)
    
    // Show summary statistics
    println("Bonus Summary Statistics:")
    bonusData.agg(
      org.apache.spark.sql.functions.sum("bonus_amount").as("total_bonuses"),
      org.apache.spark.sql.functions.avg("bonus_amount").as("avg_bonus"),
      org.apache.spark.sql.functions.min("bonus_amount").as("min_bonus"),
      org.apache.spark.sql.functions.max("bonus_amount").as("max_bonus")
    ).show()

    println("\n=== Example 9: Complex Read-Process-Write with Enrichment ===")
    println("Pipeline: Read employees + departments -> Enrich and transform -> Write to report table")
    
    // Step 1: Read from multiple JDBC tables
    spark.sparkContext.setJobDescription("JDBC Complex Pipeline: Read employees and departments")
    val employeesData = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    val departmentsData = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "departments")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    // Step 2: Process - Join and create enriched report
    spark.sparkContext.setJobDescription("JDBC Complex Pipeline: Join and enrich data")
    val enrichedReport = employeesData
      .join(departmentsData, "dept_id")
      .withColumn("salary_category",
        org.apache.spark.sql.functions.when($"salary" >= 90000, "High")
          .when($"salary" >= 70000, "Medium")
          .otherwise("Low")
      )
      .withColumn("tenure_years",
        org.apache.spark.sql.functions.floor(
          org.apache.spark.sql.functions.datediff(
            org.apache.spark.sql.functions.current_date(),
            $"hire_date"
          ) / 365
        )
      )
      .withColumn("is_senior",
        org.apache.spark.sql.functions.when($"tenure_years" >= 5, true).otherwise(false)
      )
      .select(
        $"id".as("employee_id"),
        $"name".as("employee_name"),
        $"dept_name".as("department"),
        $"location".as("office_location"),
        $"salary",
        $"salary_category",
        $"hire_date",
        $"tenure_years",
        $"is_senior"
      )
    
    println("Enriched employee report:")
    enrichedReport.orderBy(org.apache.spark.sql.functions.desc("salary")).show(10)
    
    // Step 3: Write enriched report back to JDBC
    spark.sparkContext.setJobDescription("JDBC Complex Pipeline: Write enriched report")
    enrichedReport.write
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employee_report")
      .option("user", h2User)
      .option("password", h2Password)
      .mode(SaveMode.Overwrite)
      .save()
    
    println("Successfully wrote enriched employee report to 'employee_report' table")
    
    // Query the report table with aggregations
    val reportSummary = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employee_report")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    println("\nReport Summary by Department and Salary Category:")
    reportSummary
      .groupBy("department", "salary_category")
      .agg(
        org.apache.spark.sql.functions.count("*").as("employee_count"),
        org.apache.spark.sql.functions.avg("salary").as("avg_salary")
      )
      .orderBy("department", "salary_category")
      .show()
    
    println("\nSenior Employees Summary by Office:")
    reportSummary
      .filter($"is_senior" === true)
      .groupBy("office_location")
      .agg(
        org.apache.spark.sql.functions.count("*").as("senior_count"),
        org.apache.spark.sql.functions.avg("tenure_years").as("avg_tenure")
      )
      .orderBy(org.apache.spark.sql.functions.desc("senior_count"))
      .show()

    println("\n=== Example 10: Partitioned Read with Explicit Bounds ===")
    println("Pipeline: Partition by salary column for parallel reads")
    
    spark.sparkContext.setJobDescription("JDBC: Partition by salary column")
    val partitionedBySalary = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .option("partitionColumn", "salary")
      .option("lowerBound", "50000")
      .option("upperBound", "110000")
      .option("numPartitions", "3")
      .load()
    
    println(s"Number of partitions: ${partitionedBySalary.rdd.getNumPartitions}")
    println("Partition distribution:")
    partitionedBySalary
      .groupBy(org.apache.spark.sql.functions.spark_partition_id())
      .count()
      .orderBy("spark_partition_id()")
      .show()
    
    println("\n=== Example 11: JDBC Read with Column Selection ===")
    spark.sparkContext.setJobDescription("JDBC: Read specific columns only")
    val selectedColumns = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
      .select("id", "name", "salary")
    
    println("Selected columns only (id, name, salary):")
    selectedColumns.show(5)

    println("\n=== Example 12: JDBC with WHERE Clause in dbtable ===")
    spark.sparkContext.setJobDescription("JDBC: Custom WHERE clause")
    val customWhere = """
      (SELECT * FROM employees 
       WHERE hire_date >= '2020-01-01' 
       AND dept_id IN (1, 2)
       ORDER BY hire_date DESC) AS recent_hires
    """
    
    val recentHires = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", customWhere)
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    println("Recent hires (after 2020) in Engineering and Sales:")
    recentHires.show()

    println("\n=== Example 13: JDBC Batch Insert ===")
    spark.sparkContext.setJobDescription("JDBC: Batch insert new employees")
    
    val newEmployees = Seq(
      (21, "Uma Patel", 1, 91000.00, "2025-01-15"),
      (22, "Victor Chen", 2, 74000.00, "2025-01-20"),
      (23, "Wendy Lopez", 3, 69000.00, "2025-02-01"),
      (24, "Xavier Kim", 4, 63000.00, "2025-02-10"),
      (25, "Yara Nguyen", 5, 87000.00, "2025-02-15")
    ).toDF("id", "name", "dept_id", "salary", "hire_date")
    
    newEmployees.write
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "new_employees")
      .option("user", h2User)
      .option("password", h2Password)
      .option("createTableOptions", "PRIMARY KEY (id)")
      .mode(SaveMode.Overwrite)
      .save()
    
    println("Successfully inserted new employees")
    
    val insertedEmployees = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "new_employees")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    insertedEmployees.show()

    println("\n=== Example 14: JDBC Read with fetchsize Tuning ===")
    spark.sparkContext.setJobDescription("JDBC: Read with optimized fetch size")
    val optimizedFetch = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .option("fetchsize", "10")
      .load()
    
    println(s"Read with fetchsize=10, total rows: ${optimizedFetch.count()}")
    
    println("\n=== Example 15: JDBC Read with Session Init SQL ===")
    spark.sparkContext.setJobDescription("JDBC: Read with session init SQL")
    val withSessionInit = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .option("sessionInitStatement", "SET TIME ZONE 'UTC'")
      .load()
    
    println("Read with session initialization:")
    withSessionInit.select("id", "name", "hire_date").show(5)

    println("\n=== Example 16: JDBC Aggregate Pushdown ===")
    spark.sparkContext.setJobDescription("JDBC: Aggregate with pushdown")
    val aggregateQuery = """
      (SELECT dept_id, 
              COUNT(*) as emp_count,
              AVG(salary) as avg_salary,
              MIN(hire_date) as earliest_hire,
              MAX(hire_date) as latest_hire
       FROM employees
       GROUP BY dept_id) AS dept_summary
    """
    
    val deptAggregates = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", aggregateQuery)
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    println("Department aggregates computed in database:")
    deptAggregates.show()

    println("\n=== Example 17: JDBC with Transaction Isolation ===")
    spark.sparkContext.setJobDescription("JDBC: Read with isolation level")
    val isolatedRead = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .option("isolationLevel", "READ_UNCOMMITTED")
      .load()
    
    println(s"Read with READ_UNCOMMITTED isolation: ${isolatedRead.count()} rows")

    println("\n=== Example 18: JDBC Append Mode Write ===")
    spark.sparkContext.setJobDescription("JDBC: Append additional data")
    
    val moreEmployees = Seq(
      (26, "Zoe Anderson", 1, 94000.00, "2025-03-01"),
      (27, "Aaron Blake", 2, 76000.00, "2025-03-05")
    ).toDF("id", "name", "dept_id", "salary", "hire_date")
    
    // First write (overwrite)
    moreEmployees.write
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "temp_employees")
      .option("user", h2User)
      .option("password", h2Password)
      .mode(SaveMode.Overwrite)
      .save()
    
    // Second write (append)
    val evenMoreEmployees = Seq(
      (28, "Bella Carter", 3, 71000.00, "2025-03-10")
    ).toDF("id", "name", "dept_id", "salary", "hire_date")
    
    evenMoreEmployees.write
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "temp_employees")
      .option("user", h2User)
      .option("password", h2Password)
      .mode(SaveMode.Append)
      .save()
    
    val appendedData = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "temp_employees")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    println("Data after append operations:")
    appendedData.show()

    println("\n=== Example 19: JDBC with Connection Properties ===")
    spark.sparkContext.setJobDescription("JDBC: Read with custom connection properties")
    
    val connectionProps = new java.util.Properties()
    connectionProps.put("user", h2User)
    connectionProps.put("password", h2Password)
    connectionProps.put("driver", "org.h2.Driver")
    
    val withProps = spark.read
      .jdbc(h2Url, "employees", connectionProps)
    
    println("Read using connection properties:")
    withProps.select("id", "name", "dept_id").show(5)

    println("\n=== Example 20: JDBC Partitioned Write ===")
    spark.sparkContext.setJobDescription("JDBC: Partitioned write operation")
    
    val dataToWrite = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
      .repartition(3)
    
    dataToWrite.write
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees_copy")
      .option("user", h2User)
      .option("password", h2Password)
      .option("batchsize", "5")
      .mode(SaveMode.Overwrite)
      .save()
    
    println("Successfully wrote partitioned data with batchsize=5")
    
    val copiedData = spark.read
      .format("jdbc")
      .option("url", h2Url)
      .option("dbtable", "employees_copy")
      .option("user", h2User)
      .option("password", h2Password)
      .load()
    
    println(s"Copied data count: ${copiedData.count()}")

  } finally {
    println("\n=== Press Enter to stop Spark session ===")
    scala.io.StdIn.readLine()
    spark.stop()
  }

  /**
   * Sets up H2 database with sample tables and data
   */
  def setupH2Database(): Unit = {
    var conn: Connection = null
    var stmt: Statement = null
    
    try {
      // Load H2 JDBC driver
      Class.forName("org.h2.Driver")
      
      // Create connection
      conn = DriverManager.getConnection(h2Url, h2User, h2Password)
      stmt = conn.createStatement()
      
      // Create departments table
      stmt.execute("""
        CREATE TABLE IF NOT EXISTS departments (
          dept_id INT PRIMARY KEY,
          dept_name VARCHAR(100),
          location VARCHAR(100)
        )
      """)
      
      // Create employees table
      stmt.execute("""
        CREATE TABLE IF NOT EXISTS employees (
          id INT PRIMARY KEY,
          name VARCHAR(100),
          dept_id INT,
          salary DECIMAL(10, 2),
          hire_date DATE,
          FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        )
      """)
      
      // Insert departments
      val departments = Seq(
        (1, "Engineering", "San Francisco"),
        (2, "Sales", "New York"),
        (3, "Marketing", "Los Angeles"),
        (4, "HR", "Chicago"),
        (5, "Finance", "Boston")
      )
      
      departments.foreach { case (id, name, location) =>
        stmt.execute(s"INSERT INTO departments VALUES ($id, '$name', '$location')")
      }
      
      // Insert employees
      val employees = Seq(
        (1, "Alice Johnson", 1, 95000.00, "2020-01-15"),
        (2, "Bob Smith", 1, 88000.00, "2019-03-22"),
        (3, "Carol Williams", 1, 102000.00, "2018-07-10"),
        (4, "David Brown", 2, 72000.00, "2021-02-01"),
        (5, "Eve Davis", 2, 68000.00, "2021-05-12"),
        (6, "Frank Miller", 2, 75000.00, "2020-11-30"),
        (7, "Grace Wilson", 3, 65000.00, "2022-01-20"),
        (8, "Henry Moore", 3, 70000.00, "2021-08-15"),
        (9, "Iris Taylor", 4, 58000.00, "2020-09-05"),
        (10, "Jack Anderson", 4, 62000.00, "2019-12-10"),
        (11, "Karen Thomas", 5, 85000.00, "2018-04-18"),
        (12, "Leo Jackson", 5, 92000.00, "2017-06-25"),
        (13, "Maria White", 1, 78000.00, "2022-03-01"),
        (14, "Nathan Harris", 1, 81000.00, "2021-07-14"),
        (15, "Olivia Martin", 2, 77000.00, "2020-10-08"),
        (16, "Peter Thompson", 3, 68000.00, "2021-04-22"),
        (17, "Quinn Garcia", 4, 60000.00, "2022-02-11"),
        (18, "Rachel Martinez", 5, 89000.00, "2019-11-03"),
        (19, "Samuel Robinson", 1, 96000.00, "2018-09-17"),
        (20, "Tina Clark", 2, 73000.00, "2020-05-29")
      )
      
      employees.foreach { case (id, name, deptId, salary, hireDate) =>
        stmt.execute(
          s"INSERT INTO employees VALUES ($id, '$name', $deptId, $salary, '$hireDate')"
        )
      }
      
      println("H2 Database initialized successfully with sample data")
      println(s"- Created ${departments.size} departments")
      println(s"- Created ${employees.size} employees")
      
    } catch {
      case e: Exception =>
        println(s"Error setting up H2 database: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
  }
}

