  val arrayStructureData = Seq(
    Row(Row("James ","","Smith"),List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
    Row(Row("Michael ","Rose",""),List("Tennis"),Map("hair"->"brown","eye"->"black")),
    Row(Row("Robert ","","Williams"),List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
    Row(Row("Maria ","Anne","Jones"),null,Map("hair"->"blond","eye"->"red")),
    Row(Row("Jen","Mary","Brown"),List("Blogging"),Map("white"->"black","eye"->"black"))
  )

  val arrayStructureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("hobbies", ArrayType(StringType))
    .add("properties", MapType(StringType,StringType))

  val df5 = spark.createDataFrame(
     spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  df5.printSchema()
  df5.show()
