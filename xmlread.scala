 val schema = new StructType()
      .add("_id",StringType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",StringType)
      .add("dob_month",StringType)
      .add("gender",StringType)
      .add("salary",StringType)
val df = spark.read
  .option("rowTag", "book")
  .schema(schema)
  .xml("persons.xml")
df.show()