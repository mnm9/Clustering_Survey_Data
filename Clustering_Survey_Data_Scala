%md
**Clustering of Malnutrition and Socio-Economic Factors**
%md
***TAR.GZ files for all DHS files between 2008 - 2018 for countries starting with letters from 'A' to 'N' were loaded, after which tar.gzip files are extracted 
to resources/data/ folder in DSW.***
%md
***Extract tar.gzip files that are saved on DataScientist Workbench***
%sh
tar -zxvf '/resources/data/TAR_A.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_B.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_C.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_DE.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_G1.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_G2.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_HI.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_I2.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_I3.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_I4.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_IJK.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_LM.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_N.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_PR.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_ST.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_TU.tar.gz' -C /resources/data/
tar -zxvf '/resources/data/TAR_YZ.tar.gz' -C /resources/data/

%md
***Comparing Angola (2016), Albania (2008) and Nepal (2011) files to understand the structure of data***
//variable fx with the location of the file
var angola = "/resources/data/Angola_Child_AOKR71FL_2016.csv"
var albania = "/resources/data/Albania_Child_ALKR50FL_2008.csv"
var nepal = "/resources/data/Nepal_Child_NPKR60FL_2011.csv"

//create dataframe angoladf for Angola 2016 dataset
var angoladf = spark.read
         .format("csv")
         .option("header", "true") //reading the headers
         .option("mode", "DROPMALFORMED")
         .option("inferSchema", "true") 
         .load(angola)
         
angoladf.printSchema()

//create dataframe nepaldf for Nepal 2011 dataset
var nepaldf = spark.read
         .format("csv")
         .option("header", "true") //reading the headers
         .option("mode", "DROPMALFORMED")
         .option("inferSchema", "true") 
         .load(nepal)
         
nepaldf.printSchema()

//create dataframe angoladf for Albania 2008 dataset
var albaniadf = spark.read
         .format("csv")
         .option("header", "true") //reading the headers
         .option("mode", "DROPMALFORMED")
         .option("inferSchema", "true") 
         .load(albania)
         
albaniadf.printSchema()

%md
***Merging Datasets to create a final dataset that has variables of interest***

//Create a list of files for the directory TAR_A and filter for DHS data based on filename and type
import java.io.File
val file = new File("/resources/data/")
val fileList = file.listFiles.filter(_.isFile)
    .filter(_.getName.endsWith(".csv")).filter(_.getName.contains("Child"))
    .map(_.getPath).toList

//Select variables of interest from research papers [4-7]

//Create an empty dataframe with schema
val caseid = $"caseid".string       //"Case Identification"
val midx = $"midx".int      //"Index to Birth History          
val v007 = $"v007".int       //"Year of interview"
val v012 = $"v012".int      //"Respondent's current age"
val v015 = $"v015".int      //"Result of individual interview"
val v026 = $"v026".string       //"De facto place of residence"
val v133 = $"v133".int      //"Education in single years"
val v136 = $"v136".int      //“Number of household members (listed)
val v155 = $"v155".int      //“Literacy”
val v190 = $"v190".int      //“Wealth index”
val v201 = $"v201".int      //“Total children ever born”
val v212 = $"v212".int      //“Age of respondent at first birth”
val v228 = $"v228".int      //“Ever had a terminated pregnancy”
val v445 = $"v445".int      //“Body mass index”
val v447a = $"v447a".int        //“Women's age in years (from household questionna”
val v501 = $"v501".int      //“Current marital status”
val v731 = $"v731".int      //“Respondent worked in last 12 months”
val b4 = $"b4".int      //“Sex of child”
val b5 = $"b5".int      //“Child is alive”
val b8 = $"b8".int      //“Current age of child (in months)
val hw70 = $"hw70".int      //“Height/Age standard deviation (new WHO)”
val hw71 = $"hw71".string	//“Weight/Age standard deviation (new WHO)”
val hw72 = $"hw72".string       //“Weight/Height standard deviation (new WHO)”
val hw73 = $"hw73".string       //“BMI standard deviation (new WHO)”
val filename = $"filename".string     //"filename"
val countries = $"countries".string     //"country"


import org.apache.spark.sql.types.StructType
val mySchema = StructType(caseid :: midx :: v007 :: v012 :: v015 :: v026 :: v133 :: v136 ::
v155 :: v190 :: v201 :: v212 :: v228 :: v445 :: v447a :: 
v501 :: v731 :: b4 :: b5 :: b8 :: hw70 :: hw71 :: hw72 :: hw73 :: filename :: countries :: Nil)
import org.apache.spark.sql.Row
var unionDF = spark.createDataFrame(sc.emptyRDD[Row], mySchema)
 
 
 for (f <- fileList) {
    var df= spark.read
         .format("csv")
         .option("header", "true") //reading the headers
         .option("mode", "DROPMALFORMED")
         .option("inferSchema", "true") 
         .load(f)
         .withColumn("filename",lit(f.split('/')(3)))
         .withColumn("countries",lit(f.split('/')(3).split('_')(0)))
        
    //All variable headers should be lowercase
    val cols = df.columns.map(c => s"$c as ${c.toLowerCase}")
    val df1= df.selectExpr(cols:_*)
    
    var newdf = df1.select("caseid", "midx", "v007", "v012", "v015", "v026", "v133", "v136", "v155"
    , "v190", "v201", "v212", "v228", "v445", "v447a","v501", "v731", "b4", "b5", "b8", "hw70", "hw71", "hw72", "hw73", "filename", "countries")

    unionDF = unionDF.unionAll(newdf)
    
  }

//Create a table
unionDF.createOrReplaceTempView("unionDF")

%md 
Total number of records in the merged dataset = 950,849. The final dataset includes data from 5 continents: Africa, Asia, North America, South America and Europe, and include data from 55 countries between the years of 2008-2017.
The top 3 countries with the most records are provided by countries: India (259,609), Nigeria (60,129) & Afghanistan (32,712).
The countries with the leaast records are: Albania (1,616), Sao Tome & Principe (1,931) and Guyana (2,178)
The largest number of data records come from the continent of Africa (458,397) and the least number of records come from Europe (4,813).

//Count of records in the merged dataset
%sql
SELECT COUNT(*)
FROM
unionDF

//Count of unique countries included in the dataset
%sql
SELECT COUNT(DISTINCT countries)
FROM 
unionDF

//Top 10 countries with the largest data records
%sql
SELECT countries, count(*)
FROM 
unionDF
GROUP BY countries
ORDER BY count(*) DESC
LIMIT 10

//10 Countries with the least amount of data
%sql
SELECT countries, count(*)
FROM 
unionDF
GROUP BY countries
ORDER BY count(*) ASC
LIMIT 10

%md
A csv containing list of countries and continents is loaded to aid in exploratory analysis.
%sh
wget 'https://drive.google.com/uc?export=download&id=1j3ZhkAq58WQdbj70UMcsfnTLZ07X9j9e' -O /resources/data/Countries-Continents.csv

//create dataframe continents for Countries-Continents dataset
var contDF = sqlContext.emptyDataFrame
var contDF = spark.read
         .format("csv")
         .option("header", "true") //reading the headers
         .option("mode", "DROPMALFORMED")
         .option("inferSchema", "true") 
         .load("/resources/data/Countries-Continents.csv")
         .drop("_c2").drop("_c3").drop("_c4")
         
//Create a table
contDF.createOrReplaceTempView("contDF")

//Add Continents to the countries in the unionDF dataset
var unionDF1 = unionDF.join(contDF, unionDF.col("countries")===contDF.col("Country"),"left")

//Drop variable Country, but keep variable countries
unionDF1 =unionDF1.drop(unionDF1.col("Country"))

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

//Count of data from each continent

%sql
SELECT Continent, COUNT(*)
FROM unionDF1
GROUP BY Continent
ORDER BY COUNT(*) desc

%md
**Assessment of variable v007 (Year of Interview)**

//Distribution of dataset over the years
%sql
SELECT v007, count(*)
FROM unionDF1
GROUP BY v007
ORDER BY v007 ASC

//Identifying the files and countries with a non-Gregorian calendar
%sql
SELECT v007, countries, filename, count(*)
FROM unionDF1
WHERE v007 < 2006 OR v007 > 2017
GROUP BY v007, countries, filename
ORDER BY v007 ASC

//Corrects year of interview for Afghanistan
unionDF1 = unionDF1.withColumn("v007", when(col("countries").equalTo("Afghanistan"), $"v007"+621).otherwise(col("v007")))

//Corrects year of interview for Ethiopia in file: 	Ethiopia_Child_ETKR61FL_2011.csv
unionDF1 =unionDF1.withColumn("v007", when(col("filename").equalTo("Ethiopia_Child_ETKR61FL_2011.csv"), $"v007"+8).otherwise(col("v007")))

//Corrects year of interview for Nepal
unionDF1 = unionDF1.withColumn("v007", when(col("countries").equalTo("Nepal"), $"v007"-57).otherwise(col("v007")))

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

//Check the distribution of dataset over years after the calendar correction

%sql
SELECT v007, count(*)
FROM unionDF1
GROUP BY v007
ORDER BY v007 ASC

unionDF1.describe("v007").show()

//Check the year range of the countries with a non-Gregorian calendar to validate the fix
%sql
SELECT countries, filename, v007
FROM unionDF1
WHERE countries in ('Afghanistan','Ethiopia','Nepal')
GROUP BY countries, filename, v007
ORDER BY v007 ASC

%md
**Assessment of caseid and midx**

//Removing records where midx = null
unionDF1= unionDF1.filter($"midx".isNotNull)

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%sql
SELECT midx, count(*) FROM
unionDF1 
GROUP BY midx
ORDER BY midx ASC

%md
**Assessment of Variable v012 (current age of respondent in years)**
%sql
SELECT v012, count(*)
FROM unionDF1
WHERE v012 IS NULL
GROUP BY v012

%sql
SELECT v012, count(*) FROM 
unionDF1
GROUP BY v012
ORDER BY v012 ASC

%sql
SELECT FLOOR(v012/10), count(*) FROM 
unionDF1
GROUP BY FLOOR(v012/10)
ORDER BY FLOOR(v012/10) ASC

unionDF1.describe("v012").show()

%md
**Assessment of Variable v015 (Result of individual interview)**
%sql
SELECT v015, count(*)
FROM unionDF1 
WHERE v015 <> 1
GROUP BY v015

%md
**Assessment of Variable v026 (De facto place of residence)**
%sql
SELECT v026, count(*)
FROM unionDF1
GROUP BY v026
order by v026 ASC

//Drop variable v026 
unionDF1 =unionDF1.drop(unionDF1.col("v026"))

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%md
**Assessment of Variable v133 (Education in single years)**
%sql
SELECT v133, COUNT(*)
FROM unionDF1
GROUP BY v133
ORDER BY v133 ASC

//Removing records where v133 = null
unionDF1= unionDF1.filter($"v133".isNotNull)

//Removing 
unionDF1 = unionDF1.filter("v133 <> 99 or v133 <> 98.0 or v133 <> 99.0")

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%sql
SELECT CAST(v133 AS INT), COUNT(*) 
FROM unionDF1
GROUP BY CAST(v133 AS INT)
ORDER BY CAST(v133 AS INT) ASC

unionDF1.describe("v133").show()

%md
**Assessment of Variables v136 (Number of household members (listed))**
%sql
SELECT v136, count(*) FROM
unionDF1
GROUP BY v136 
ORDER BY v136 ASC

unionDF1.describe("v136").show()
%md
**Assessment of Variable v155 (Literacy)**
//Removing records where v155 = null
unionDF1= unionDF1.filter($"v155".isNotNull)

//This variable is converted to a binary field: v155_literate where labels (0, 3 and 4) are recoded as 0, whereas labels (1,2) are recoded as 1.
unionDF1 = unionDF1.withColumn("v155_literate", when($"v155" === 0 or $"v155" ===3 or $"v155" ===4 ,0).otherwise(1))

//Drop variable v155 
unionDF1 =unionDF1.drop(unionDF1.col("v155"))

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%sql
SELECT v155_literate, count(*)
from unionDF1
GROUP BY v155_literate
ORDER BY v155_literate

%md
**Assessment of Variable v190 (Wealth Index)**
%sql
SELECT v190, count(*) FROM unionDF1
GROUP BY v190
ORDER BY v190 ASC

unionDF1.describe("v190").show()

%md 
**Assessment of Variable v201 (Total children ever born)**
%sql
SELECT v201, count(*)
FROM unionDF1
GROUP BY v201
ORDER BY v201 ASC

unionDF1.describe("v201").show()

%md
**Assessment of Variable v212 (Age of respondent at first birth)**
%sql
SELECT v212, count(*)
FROM unionDF1
GROUP BY v212
ORDER BY v212 ASC

unionDF1.describe("v212").show()

%md
**Assessment of Variable v228 (Ever had a terminated preganancy)**
%sql 
SELECT v228, count(*)
FROM unionDF1
GROUP BY v228
ORDER BY v228 ASC

//Removing records where v228 = null
unionDF1= unionDF1.filter($"v228".isNotNull)

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%sql
SELECT v228, count(*)
FROM unionDF1
GROUP BY v228
ORDER BY v228 ASC

%md
**Assessment of Variable v445 (Body mass index)**

%sql
SELECT v445, count(*)
FROM unionDF1
WHERE v445 is null
GROUP BY v445
ORDER BY v445 ASC

//Drop variable v445 
unionDF1 =unionDF1.drop(unionDF1.col("v445"))

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%md
**Assessment of Variable v447a (Women's age in years)**

%sql
SELECT v447a, count(*)
FROM unionDF1
GROUP BY v447a
ORDER BY v447a ASC

//Drop variable v447a
unionDF1 =unionDF1.drop(unionDF1.col("v447a"))

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%md
**Assessment of Variable v501 (Current Marital Status)**
%sql
SELECT v501, count(*)
FROM unionDF1
GROUP BY v501
ORDER BY v501 ASC

//Removing records where v501 = null
unionDF1= unionDF1.filter($"v501".isNotNull)

//This variable is converted to binary variable v501_married_partner where fields 1 & 2 = 1, and 0,3, 4 and 5 = 0
unionDF1 = unionDF1.withColumn("v501_married_partner", when($"v501" === 1 or $"v501" ===2 ,1).otherwise(0))

//Drop variable v155 
unionDF1 =unionDF1.drop(unionDF1.col("v501"))

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%sql 
SELECT v501_married_partner, count(*)
FROM unionDF1
GROUP BY v501_married_partner

%md
**Assessment of Variable v731 (Respondent worked in last 12 years)**
%sql
SELECT v731, count(*)
FROM unionDF1
GROUP BY v731
ORDER BY v731 ASC

//Removing records where v731 = null
unionDF1= unionDF1.filter($"v731".isNotNull)

//This variable is converted to binary variable v731_working where field, 0 = 0 and 1,2 & 3= 1
unionDF1 = unionDF1.withColumn("v731_working", when($"v731" === 0,0).otherwise(1))

//Drop variable v731 
unionDF1 =unionDF1.drop(unionDF1.col("v731"))

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%sql
SELECT v731_working, count(*)
FROM unionDF1
GROUP BY v731_working
ORDER BY v731_working ASC

%md
**Assessment of Variable b4 (Sex of child)**

%sql
SELECT b4, count(*)
from unionDF1
group by b4
order by b4 asc

//Removing records where b4 = null
unionDF1= unionDF1.filter($"b4".isNotNull)

//This variable is converted to b4_male where 1 = 1 and 2 =0.  
unionDF1 = unionDF1.withColumn("b4_male", when($"b4" === 1,1).otherwise(0))

//Drop variable b4 
unionDF1 =unionDF1.drop(unionDF1.col("b4"))

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%sql
SELECT b4_male, count(*)
from unionDF1
group by b4_male
order by b4_male asc

%md
**Assessment of Variable b5 (Child is Alive)**

%sql
SELECT b5, count(*)
FROM unionDF1
GROUP BY b5
ORDER BY b5 ASC

//filter out all records where child is not alive (b5 =0)
unionDF1 = unionDF1.filter("b5 <> 0")

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

%md
**Assessment of Variable b8 (Current age of child)**

%sql
SELECT b8, count(*)
FROM unionDF1
GROUP BY b8
ORDER BY b8 ASC

unionDF1.describe("b8").show()

%md
**Assessment of Variable hw70 (Height/Age standard deviation (new WHO), Variable hw71 (Weight/Age standard deviation (new WHO) and Variable hw72 (Weight/Height standard deviation (new WHO)**

//filter out all records where hw70, hw71 and hw72 is null
unionDF1 = unionDF1.filter("hw70 is not null and hw71 is not null and hw72 is not null")

unionDF1 = unionDF1.filter("hw70 <> '9999.0' and hw71 <> '9999.0' and hw72 <> '9999.0'")

unionDF1 = unionDF1.filter("hw70 <> '9999.0'")   
unionDF1 = unionDF1.filter("hw70 <> '9998.0'")
unionDF1 = unionDF1.filter("hw70 <> '9998'")
unionDF1 = unionDF1.filter("hw70 <> '9997.0'")
unionDF1 = unionDF1.filter("hw70 <> '9997'")
unionDF1 = unionDF1.filter("hw70 <> '9996.0'")
unionDF1 = unionDF1.filter("hw70 <> '9996'")

unionDF1 = unionDF1.filter("hw71 <> '9999.0'")   
unionDF1 = unionDF1.filter("hw71 <> '9998.0'")
unionDF1 = unionDF1.filter("hw71 <> '9998'")
unionDF1 = unionDF1.filter("hw71 <> '9997.0'")
unionDF1 = unionDF1.filter("hw71 <> '9997'")
unionDF1 = unionDF1.filter("hw71 <> '9996.0'")
unionDF1 = unionDF1.filter("hw71 <> '9996'")

unionDF1 = unionDF1.filter("hw72 <> '9999.0'")   
unionDF1 = unionDF1.filter("hw72 <> '9998.0'")
unionDF1 = unionDF1.filter("hw72 <> '9998'")
unionDF1 = unionDF1.filter("hw72 <> '9997.0'")
unionDF1 = unionDF1.filter("hw72 <> '9997'")
unionDF1 = unionDF1.filter("hw72 <> '9996.0'")
unionDF1 = unionDF1.filter("hw72 <> '9996'")


//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

//Divide hw70/100 to create a new columns
unionDF1 = unionDF1.withColumn("hw70_dec", $"hw70"/100)

//Divide hw71/100 to create a new columns
unionDF1 = unionDF1.withColumn("hw71_dec", $"hw71"/100)

//Divide hw72/100 to create a new columns
unionDF1 = unionDF1.withColumn("hw72_dec", $"hw72"/100)

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

//Distribution of HAZ
%sql
SELECT CAST(FLOOR(hw70_dec) AS DOUBLE) AS hw_70_bin, count(*)
FROM unionDF1
GROUP BY CAST(FLOOR(hw70_dec) AS DOUBLE)
ORDER BY hw_70_bin ASC

//Distribution of WAZ
%sql
SELECT CAST(FLOOR(hw71_dec) AS DOUBLE) AS hw_71_bin, count(*)
FROM unionDF1
GROUP BY CAST(FLOOR(hw71_dec) AS DOUBLE)
ORDER BY hw_71_bin ASC

//Distribution of WHZ
%sql
SELECT CAST(FLOOR(hw72_dec) AS DOUBLE) AS hw_72_bin, count(*)
FROM unionDF1
GROUP BY CAST(FLOOR(hw72_dec) AS DOUBLE)
ORDER BY hw_72_bin ASC

%md
**Assessment of Variable hw73 (BMI standard deviation (new WHO))**
//filter out all records where hw73 is null
unionDF1 = unionDF1.filter("hw73 is not null")

unionDF1 = unionDF1.filter("hw73 <> '9999.0'")   
unionDF1 = unionDF1.filter("hw73 <> '9998.0'")
unionDF1 = unionDF1.filter("hw73 <> '9998'")
unionDF1 = unionDF1.filter("hw73 <> '9997.0'")
unionDF1 = unionDF1.filter("hw73 <> '9997'")
unionDF1 = unionDF1.filter("hw73 <> '9996.0'")
unionDF1 = unionDF1.filter("hw73 <> '9996'")

//Divide hw73/100 to create a new columns
unionDF1 = unionDF1.withColumn("hw73_dec", $"hw73"/100)

//Create a table
unionDF1.createOrReplaceTempView("unionDF1")

//Distribution of BMI
%sql
SELECT CAST(FLOOR(hw73_dec) AS DOUBLE) AS hw_73_bin, count(*)
FROM unionDF1
GROUP BY CAST(FLOOR(hw73_dec) AS DOUBLE)
ORDER BY hw_73_bin ASC

//Correlation between HAZ & WAZ
%sql
SELECT CORR(hw70_dec, hw71_dec)
FROM unionDF1

//Correlation between HAZ & BMI
%sql
SELECT CORR(hw70_dec, hw73_dec)
FROM unionDF1

//Correlation between HAZ & BMI
%sql
SELECT T1.countries, ROUND((T1.COUNT_OF_NEG2HAZ/T2.COUNT_OF_HAZ)*100,2) AS PERC_STUNTING
FROM
(SELECT countries, count(*) AS COUNT_OF_NEG2HAZ
FROM unionDF1
WHERE hw70_dec <= -2
GROUP BY countries) T1
LEFT JOIN 
(SELECT countries, count(*) AS COUNT_OF_HAZ
FROM unionDF1
GROUP BY countries) T2
ON T1.countries = T2.COUNTRIES
ORDER BY PERC_STUNTING DESC
LIMIT 10

//Top 10 countries with highest wastin
%sql
SELECT T1.countries, ROUND((T1.COUNT_OF_NEG2WAZ/T2.COUNT_OF_WAZ)*100,2) AS PERC_WASTING
FROM
(SELECT countries, count(*) AS COUNT_OF_NEG2WAZ
FROM unionDF1
WHERE hw71_dec <= -2
GROUP BY countries) T1
LEFT JOIN 
(SELECT countries, count(*) AS COUNT_OF_WAZ
FROM unionDF1
GROUP BY countries) T2
ON T1.countries = T2.COUNTRIES
ORDER BY PERC_WASTING DESC
LIMIT 10

%md
**K-Means Clustering**

K-means clustering on DSW is very slow on such a large dataset (3+ hours to run a loop for different numbers of k). Therefore to demonstrate how k-means would work, I am taking a subset of data for the countries with the top stunting and wasting rates: Timor Leste, Niger, Guatemala, Pakistan and Benin.

//Create a dataframe for 3 selected variables that are to be included in the clustering model
val rdd_df = sqlContext.sql("select countries, Continent, v012, v133, v136, v155_literate, v190, v201, v212, v501_married_partner, v731_working, b4_male, b8, hw70_dec, hw71_dec, hw72_dec, hw73_dec from unionDF1 where countries in ('TimorLeste','Niger', 'Guatemala', 'Pakistan','Benin')")

rdd_df.printSchema()
//import and use Vector Assembler to create Column: Features
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

val assembler = new VectorAssembler()
  .setInputCols(Array("v012", "v136", "v155_literate", "v190", "v201", "v212", "v501_married_partner", "v731_working", "b4_male", "b8"))
  .setOutputCol("features")
  
//Output includes "features" column
val output = assembler.transform(rdd_df)

//See the output df
output.select("features").show(false)

//Create a table
output.createOrReplaceTempView("output")

%sql
SELECT distinct countries FROM output

//import library for kmeans
import org.apache.spark.ml.clustering.KMeans
//Create an empty dataframe with schema
val countries = $"countries".string       //"Countries"
val Continent = $"Continent".string     //"Countinent"
val v012 = $"v012".int		//"current age of respondent in years"
val v133 = $"v133".int      //"Education in single years"          
val v136 = $"136".int      //“Number of household members (listed)"
val v155_literate = $"v155_literate".int        //Literate
val v190 = $"v190".int       //Wealth index
val v201 = $"v201".int      //Total children ever born
val v212 = $"v212".int      //age of respondent at first birth
val v501_married_partner = $"v501_married_partner".int      //currenty marital status
val v731_working = $"v731_working".int      //Respondent worked in last 12 months
val b4_male = $"b4_male".int        //Sex of the child
val b8 = $"b8".int      //Age of the child
val hw70_dec = $"hw70_dec".int      //Height/Age SD (new WHO)
val hw71_dec = $"hw71_dec".int      //Weight/Age SD (new WHO)
val hw72_dec = $"hw72_dec".int      //Weight/Height SD (new WHO)
val hw73_dec = $"hw73_dec".int      //BMI SD (new WHO)
val prediction = $"prediction".int 	//"prediction"
val number_of_clusters = $"number_of_clusters".int //"Number of clusters"
val WSSSE_val = $"WSSSE_val".int    //WSSSE score

import org.apache.spark.sql.types.StructType

val mySchema = StructType(countries :: Continent :: v012 :: v133 :: v136 :: v155_literate :: v190 :: v201 :: v212 :: v501_married_partner :: v731_working :: b4_male :: b8 :: hw70_dec :: hw71_dec :: hw72_dec :: hw73_dec :: prediction :: number_of_clusters :: WSSSE_val :: Nil)

import org.apache.spark.sql.Row

var unionDFfeatures = spark.createDataFrame(sc.emptyRDD[Row], mySchema)

val cl = 30

for (i <- Range(5, cl, 10))
{

val kmeans = new KMeans().setK(i).setSeed(1L)
val model = kmeans.fit(output)
val predictions = model.transform(output)
val WSSSE = model.computeCost(output)

//predictions.printSchema()
var newdf = predictions.select("countries", "Continent", "v012", "v133", "v136", "v155_literate", "v190", "v201", "v212", "v501_married_partner", "v731_working", "b4_male", "b8","hw70_dec", "hw71_dec", "hw72_dec", "hw73_dec" , "prediction")

val newdf1 = newdf.withColumn("number_of_clustes", lit(i))
val newdf2 = newdf1.withColumn("WSSSE_val", lit(WSSSE))

unionDFfeatures = unionDFfeatures.unionAll(newdf2)


}

//Create a table
unionDFfeatures.createOrReplaceTempView("unionDFfeatures")

//Scree plot to identify number of clusters

%sql
SELECT number_of_clusters, WSSSE_val FROM unionDFfeatures
GROUP by number_of_clusters, WSSSE_val
ORDER BY number_of_clusters ASC

//Re-running the cluster for optimal # of clusters
val kmeans = new KMeans().setK(15).setSeed(1L)
val model = kmeans.fit(output)
val predictions = model.transform(output)
val WSSSE = model.computeCost(output)

// Shows the result.
println("Cluster Centers: ")
model.clusterCenters.foreach(println)
