# -------------------------------------------------------
#' 
#' Difinity Conference 2018, Auckland New Zealand
#' 
#' Scalable Data Science with SparkR on HDInsight
#' By Lace Lofranco
#' 
#' Demo scripts - 1: Hello, SparkR: Data Cleaning
#' 
#' -----------------------------------------------------


# Load SparkR package --------------------
library(SparkR, lib.loc = paste0(Sys.getenv("SPARK_HOME"), "/R/lib/"))



# Start and Stop a session --------------------

# Start
sparkR.session()

# Inspect session
sparkR.conf()

# Check spark version
sparkR.version()

# Stop 
sparkR.session.stop()



# Start a custom session ---------------

sparkR.session(master = "yarn",
               sparkConfig = list('spark.executor.memory' = '10g',
                                  'spark.executor.instances' = '18',
                                  'spark.executor.cores' = '4',
                                  'spark.driver.memory' = '4g'))
# Log level
setLogLevel("ERROR")



# Read data (w/ schema) -------------------------

schema <- structType(structField("BibNumber", "integer"), 
                     structField("ItemBarcode", "string"),
                     structField("ItemType", "string"),
                     structField("Collection", "string"),
                     structField("CallNumber", "string"),
                     structField("CheckoutDateTime", "string"))

sdfCheckouts <- SparkR::read.df("/data/seattle-library/Checkouts_By_Title_Data_Lens/",
                                source = "csv",
                                header = "true",
                                schema = schema,
                                na.strings = "NA")

# Lazy spark :)
nrow(sdfCheckouts)



# Read data (w/o schema) -------------------------

# Library Collection
sdfCollection <- SparkR::read.df("/data/seattle-library/Library_Collection_Inventory.csv",
                                 source = "csv",
                                 header = "true",
                                 inferSchema = "true",
                                 quote = '"')

# Library Data Dictionary
sdfIls <- SparkR::read.df("/data/seattle-library/Integrated_Library_System__ILS__Data_Dictionary.csv",
                          source = "csv",
                          header = "true",
                          inferSchema = "true")



# Convert Spark dataframe to R dataframe -------------------------

rdfIls <- SparkR::collect(sdfIls)

View(rdfIls)



# Convert R dataframe into Spark dataframe -------------------------

# Build an R dataframe - full year-weeks 
rdfDates <- data.frame(CheckoutDate = seq(from = as.Date("2005-01-01"), to = as.Date("2017-12-31"), by = "days"))

# Convert into Spark dataframe
sdfDates <- SparkR::createDataFrame(rdfDates) 



# Clean data * -------------------------------------

#######################
# CHECKOUTS

# Convert string to timestamp
sdfCheckouts$CheckoutDateTime <- cast(unix_timestamp(sdfCheckouts$CheckoutDateTime, 'MM/dd/yyyy hh:mm:ss a'), "timestamp")
sdfCheckouts$CheckoutDate <- SparkR::date_format(sdfCheckouts$CheckoutDateTime, "yyyy-MM-dd")
sdfCheckouts$CheckoutDate <- SparkR::to_date(sdfCheckouts$CheckoutDate)
sdfCheckouts$ItemTypeCollectionKey <- SparkR::concat(sdfCheckouts$ItemType, lit("_"), sdfCheckouts$Collection)


#######################
# COLLECTION

# Convert ReportDate string to date
sdfCollection$ReportDate <- SparkR::to_date(cast(unix_timestamp(sdfCollection$ReportDate, 'MM/dd/yyyy'), "timestamp"))

# Clean sdfCollection
sdfCollection$Publisher <- SparkR::regexp_replace(sdfCollection$Publisher, ',\\s*$|\\s+.\\s*$', '') # Remove trailing commas, trailing periods with a space before it -- after initial clean up

# Clean PublicationYear
sdfCollection$PublicationYear <- SparkR::regexp_extract(sdfCollection$PublicationYear, '\\d{4}', 0)
sdfCollection$PublisherYear <- SparkR::cast(sdfCollection$PublicationYear, "int")

# Clean Author
sdfCollection$Author <- SparkR::regexp_replace(sdfCollection$Author, '\\d{4}|-|,{2,}|-\\d*|,\\s*$', '') # Remove years, hyphens, two or more commas together, and trailing commas
sdfCollection$Author <- SparkR::regexp_replace(sdfCollection$Author, ',\\s*$|\\s+.\\s*$', '') # Remove trailing commas, trailing periods with a space before it -- after initial clean up

# Convert Floating to boolean
sdfCollection$FloatingItem <- SparkR::ifelse(sdfCollection$FloatingItem == "Floating", 1, 0)

# Filter out not null ReportDate to remove bad rows.
sdfCollection <- sdfCollection %>% SparkR::filter(SparkR::isNotNull(.$ReportDate))

# Drop duplicates
sdfCollection <- SparkR::dropDuplicates(sdfCollection)

# Note that collection data is appended on each Report Date
# Get max report date to filter to only max reporting period
rdfMaxReportDate <- sdfCollection %>%
  SparkR::select(.$ReportDate) %>%
  SparkR::orderBy(SparkR::desc(.$ReportDate)) %>%
  SparkR::head(1)
sdfCollection <- sdfCollection %>%
  SparkR::filter(.$ReportDate == rdfMaxReportDate$ReportDate)

# Unfortunately checkout data does not capture location
# Dropping location data
sdfCollectionLoc <- sdfCollection %>%
  SparkR::groupBy(.$BibNum, .$Title, .$Author, .$ISBN, .$PublicationYear, .$Publisher, .$Subjects, .$ItemType, .$ItemCollection, .$FloatingItem) %>%
  SparkR::agg(ItemCount = sum(sdfCollection$ItemCount)) %>%
  SparkR::drop(sdfCollection$ItemLocation)

#######################
# ILS

# Rename column names
colnames(sdfIls) <- c("Code", "Description", "CodeType", "FormatGroup", "FormatSubgroup", "CategoryGroup", "CategorySubgroup")

# Drop duplicates
sdfIls <- SparkR::dropDuplicates(sdfIls)

#######################
# ILS - ItemType

sdfIlsItemType <- sdfIls %>% 
  SparkR::filter(.$CodeType == "ItemType")

# Add NoHold, Adult, Young Adult, Juvenile, FormerlyNoFine, Reference columns
sdfIlsItemType$NoHold <- SparkR::ifelse(SparkR::instr(SparkR::lower(sdfIlsItemType$Description), "no hold") == 0, 0, 1)
sdfIlsItemType$Adult <- SparkR::ifelse(SparkR::instr(SparkR::lower(sdfIlsItemType$Description), "adult") == 0, 0, 1)
sdfIlsItemType$YoungAdult <- SparkR::ifelse(SparkR::instr(SparkR::lower(sdfIlsItemType$Description), "ya") == 0, 0, 1)
sdfIlsItemType$Juvenile <- SparkR::ifelse(SparkR::instr(SparkR::lower(sdfIlsItemType$Description), "juv") == 0, 0, 1)
sdfIlsItemType$FormerlyNoFine <- SparkR::ifelse(SparkR::instr(SparkR::lower(sdfIlsItemType$Description), "formerly no fine") == 0, 0, 1)
sdfIlsItemType$Reference <- SparkR::ifelse(sdfIlsItemType$CategoryGroup == "Reference", 1, 0)

# Fill FormatSubgroup
# sdfIlsItemType <- sdfIlsItemType %>% fillna("unclassified", cols = list("FormatSubGroup"))

# Remove null columns
sdfIlsItemType <- sdfIlsItemType %>% SparkR::drop(c("CodeType", "CategoryGroup", "CategorySubGroup"))

# Rename columns
sdfIlsItemType <- sdfIlsItemType %>% SparkR::withColumnRenamed("Code", "ItemType")
sdfIlsItemType <- sdfIlsItemType %>% SparkR::withColumnRenamed("Description", "TypeDescription")
sdfIlsItemType <- sdfIlsItemType %>% SparkR::withColumnRenamed("FormatGroup", "TypeFormatGroup")
sdfIlsItemType <- sdfIlsItemType %>% SparkR::withColumnRenamed("FormatSubgroup", "TypeFormatSubGroup")

#######################
# ILS - ItemCollection

sdfIlsItemCollection <- sdfIls %>%SparkR::filter(.$CodeType == "ItemCollection")

# Remove columns
sdfIlsItemCollection <- sdfIlsItemCollection %>% SparkR::drop(c("CodeType", "FormatGroup", "FormatSubgroup"))

# Rename columns
sdfIlsItemCollection <- sdfIlsItemCollection %>% SparkR::withColumnRenamed("Code", "ItemCollection")
sdfIlsItemCollection <- sdfIlsItemCollection %>% SparkR::withColumnRenamed("Description", "ColDescription")
sdfIlsItemCollection <- sdfIlsItemCollection %>% SparkR::withColumnRenamed("CategoryGroup", "ColCategoryGroup")
sdfIlsItemCollection <- sdfIlsItemCollection %>% SparkR::withColumnRenamed("CategorySubgroup", "ColCategorySubGroup")



# Write data --------------------------------

outBaseDir <- "demo_1/"

write.df(sdfCheckouts, paste0(outBaseDir, "checkouts_by_title_data_lens"), "parquet", "overwrite")
write.df(sdfCollection, paste0(outBaseDir, "library_collection_inventory"), "parquet", "overwrite")
write.df(sdfDates, paste0(outBaseDir, "dates"), "parquet", "overwrite")
write.df(sdfIlsItemType, paste0(outBaseDir, "ils_item_type"), "parquet", "overwrite")
write.df(sdfIlsItemCollection, paste0(outBaseDir, "ils_item_collection"), "parquet", "overwrite")

