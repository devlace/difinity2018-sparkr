# -------------------------------------------------------
#' 
#' Difinity Conference 2018, Auckland New Zealand
#' 
#' Scalable Data Science with SparkR on HDInsight
#' By Lace Lofranco
#' 
#' Demo scripts - 0: Setup
#' 
#' -------------------------------------------------------

# Restart R
.rs.restartR()

# Clear memory
rm(list=ls())

# Install packages
packages <- c("ggplot2", "dplyr", "lubridate")
if (length(setdiff(packages, rownames(installed.packages()))) > 0) {
  install.packages(setdiff(packages, rownames(installed.packages())))  
}

library(dplyr)
library(ggplot2)


####

library(SparkR, lib.loc = paste0(Sys.getenv("SPARK_HOME"), "/R/lib/"))

# Session
sparkR.session.stop()
sparkR.session(master = "yarn",
               sparkConfig = list('spark.executor.memory' = '10g',
                                  'spark.executor.instances' = '18',
                                  'spark.executor.cores' = '4',
                                  'spark.driver.memory' = '4g'))
# Log level
setLogLevel("ERROR")
