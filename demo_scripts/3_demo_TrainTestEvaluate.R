#' ######################################################
#' 
#' Difinity Conference 2018, Auckland New Zealand
#' 
#' Scalable Data Science with SparkR on HDInsight
#' By Lace Lofranco
#' 
#' Demo scripts - 2: Machine Learning
#' 
#' ######################################################


# Helper functions * --------------------------------------------------

evaluatePredictions <- function(sdfPredictions) {
  # Calculate TEST set R2, SSE, and SST
  avgLabel <- sdfPredictions %>% SparkR::select(avg(.$label)) %>% collect()
  sdfSS <- sdfPredictions %>% 
    SparkR::select(alias(sum( (.$prediction - .$label)^2 ), "sse"),
                   alias(sum( (lit(avgLabel[[1]]) - .$label)^2 ), "sst")) 
  rdfResults <- sdfSS %>% 
    SparkR::mutate(R2 = lit(1.0) - (.$sse / .$sst)) %>%
    head(1)
  
  return(rdfResults)
}


# Read data ------------------------------------------------
sdfCheckoutsDaily <- read.parquet("demo_2/checkouts_daily")

# View data
sdfCheckoutsDaily %>% head(100) %>% View()


# Split into Train and Test ---------------------------------

# Random split:
# sdfSplit <- sdfCheckoutsDaily %>% randomSplit(c(7, 3), 0)
# sdfTrain <- sdfSplit[[1]]
# sdfTest <- sdfSplit[[2]]

# Split by date
sdfTrain <- sdfCheckoutsDaily %>% SparkR::filter(.$CheckoutDate >= "2005-01" & .$CheckoutDate < "2014-01") %>% SparkR::cache()
sdfTest <- sdfCheckoutsDaily %>% SparkR::filter(.$CheckoutDate >= "2014-01")  %>% SparkR::cache()


# Model - GLM --------------------------------------------------

modelLr <- spark.glm(sdfTrain, CheckoutCount ~ . - CheckoutDate - ItemTypeCollectionKey, 
                     family = "gaussian", regParam = 0.3)

# View internals of the model
summary(modelLr)

# Make predictions
sdfPredictions <- predict(modelLr, newData = sdfTest)

# Evaluate
rdfModelScoreLr <- evaluatePredictions(sdfPredictions)
View(rdfModelScoreLr)


# Model - RandomForest --------------------------------------------------

modelRf <- spark.randomForest(sdfTrain, 
                              CheckoutCount ~ . - CheckoutDate - ItemTypeCollectionKey, 
                              type = "regression", maxDepth = 5, maxBins = 16)

# View internals of the model
summary(modelRf)

# Make predictions
sdfPredictions <- predict(modelRf, newData = sdfTest)

# Evaluate
rdfModelScoreRf <- evaluatePredictions(sdfPredictions)
View(rdfModelScoreRf)



# Save model ------------------------------------------------

outBaseDir <- "demo_3/"
# Persist model
write.ml(modelLr, paste0(outBaseDir, "model_glm_1"), overwrite = TRUE)
write.ml(modelRf, paste0(outBaseDir, "model_randomForest_1"), overwrite = TRUE)
