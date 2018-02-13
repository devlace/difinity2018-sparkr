# -------------------------------------------------------
#' 
#' Difinity Conference 2018, Auckland New Zealand
#' 
#' Scalable Data Science with SparkR on HDInsight
#' By Lace Lofranco
#' 
#' Demo scripts - 2: Feature Engineering
#' 
#' -----------------------------------------------------

# Read data ----------------------------------
inBaseDir <- "demo_1/"
sdfCheckouts <- read.parquet(paste0(inBaseDir, "checkouts_by_title_data_lens"))
sdfCollection <- read.parquet(paste0(inBaseDir, "library_collection_inventory"))
sdfIlsItemType <- read.parquet(paste0(inBaseDir, "ils_item_type"))
sdfIlsItemCollection <- read.parquet(paste0(inBaseDir, "ils_item_collection"))
sdfDates <- read.parquet(paste0(inBaseDir, "dates"))

# Explore data * -------------------------------

multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
  
  # source: http://www.cookbook-r.com/Graphs/Multiple_graphs_on_one_page_(ggplot2)/
  
  library(grid)
  
  # Make a list from the ... arguments and plotlist
  plots <- c(list(...), plotlist)
  
  numPlots = length(plots)
  
  # If layout is NULL, then use 'cols' to determine layout
  if (is.null(layout)) {
    # Make the panel
    # ncol: Number of columns of plots
    # nrow: Number of rows needed, calculated from # of cols
    layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                     ncol = cols, nrow = ceiling(numPlots/cols))
  }
  
  if (numPlots==1) {
    print(plots[[1]])
    
  } else {
    # Set up the page
    grid.newpage()
    pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
    
    # Make each plot, in the correct location
    for (i in 1:numPlots) {
      # Get the i,j matrix positions of the regions that contain this subplot
      matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
      
      print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                      layout.pos.col = matchidx$col))
    }
  }
}

th <- theme_grey()

# Num of checkouts per DoY
sdfByDoY <- sdfCheckouts %>%
  SparkR::groupBy(.$CheckoutDate) %>%
  SparkR::agg(CheckoutCount = SparkR::count(sdfCheckouts$ItemBarcode)) %>%
  SparkR::arrange(SparkR::asc(.$CheckoutDate))
rdfByDoY <- collect(sdfByDoY)
p1 <- ggplot(rdfByDoY, aes(CheckoutDate, CheckoutCount, colour = CheckoutCount)) + 
  geom_line() +
  scale_x_date(date_breaks = "1 year") +
  ggtitle("# of daily checkouts by date") + xlab("Checkout Date") +  ylab("# of daily checkouts") +
  stat_smooth(method = "loess", formula = y ~ x, size = 1.5, colour = "orange", level = 0.99) + 
  th + theme(axis.text.x = element_text(angle = -60, hjust = 0), legend.position="none")

# Num of checkouts per DoY by top item types
sdfByDoYType <- sdfCheckouts %>%
  SparkR::filter(.$ItemType %in% c('acbk', 'acdvd', 'jcbk', 'accd', 'jcdvd')) %>%
  SparkR::groupBy(.$CheckoutDate, .$ItemType) %>%
  SparkR::agg(CheckoutCount = SparkR::count(sdfCheckouts$ItemBarcode)) %>%
  SparkR::arrange(SparkR::asc(.$CheckoutDate))
rdfByDoYType <- collect(sdfByDoYType)
p2 <- ggplot(rdfByDoYType, aes(CheckoutDate, CheckoutCount, colour = ItemType)) + 
  geom_line() +
  facet_grid(ItemType ~ .) +
  scale_x_date(date_breaks = "1 year") +
  ggtitle("# of daily checkouts per day for top book types") + xlab("Checkout Date") +  ylab("# of daily checkouts") +
  th + theme(axis.text.x = element_text(angle = -60, hjust = 0))

# Num of checkouts per ItemType
sdfByItemType <- sdfCheckouts %>%
  SparkR::groupBy(.$ItemType) %>%
  SparkR::agg(CheckoutCount = SparkR::count(sdfCheckouts$ItemBarcode)) %>%
  SparkR::arrange(SparkR::desc(.$CheckoutCount))
rdfByItemType <- head(sdfByItemType, 30)
rdfByItemType$ItemType <- factor(rdfByItemType$ItemType, levels = rdfByItemType$ItemType[order(-rdfByItemType$CheckoutCount)])
p3 <- ggplot(rdfByItemType, aes(ItemType, CheckoutCount)) + 
  geom_bar(stat = "identity") +
  ggtitle("Top 30 book types by # of checkouts") + xlab("Book Item Type") + ylab("# of daily checkouts") +
  th + theme(axis.text.x = element_text(angle = -60, hjust = 0))


# Num of checkouts per ItemCollection
sdfByCollection <- sdfCheckouts %>%
  SparkR::groupBy(.$Collection) %>%
  SparkR::agg(CheckoutCount = SparkR::count(sdfCheckouts$ItemBarcode)) %>%
  SparkR::arrange(SparkR::desc(.$CheckoutCount))
rdfByCollection <- head(sdfByCollection, 30)
rdfByCollection$Collection <- factor(rdfByCollection$Collection, levels = rdfByCollection$Collection[order(-rdfByCollection$CheckoutCount)])
p4 <- ggplot(rdfByCollection, aes(Collection, CheckoutCount)) + 
  geom_bar(stat = "identity") +
  ggtitle("Top 30 book collection by # of checkouts") + xlab("Book Item Collection") +ylab("# of daily checkouts") +
  th + theme(axis.text.x = element_text(angle = -60, hjust = 0))

multiplot(p1, p3, p2, p4, cols=2)



# Feature Engineering - Window Functions --------------------------------

# Collapse to Daily
sdfCheckoutsDaily <- sdfCheckouts %>%
  SparkR::groupBy(.$ItemTypeCollectionKey, .$CheckoutDate) %>%
  SparkR::agg(CheckoutCount = SparkR::count(sdfCheckouts$ItemBarcode))

# Add Daily rolling avg and std (past 3 days)
frame3Day <- windowPartitionBy("ItemTypeCollectionKey") %>% 
  orderBy("CheckoutDate") %>% 
  rowsBetween(-3, 0)

# Add daily rolling avg and std (past 15 days)
frame15Day <- windowPartitionBy("ItemTypeCollectionKey") %>% 
  orderBy("CheckoutDate") %>% 
  rowsBetween(-15, 0)

# Add daily rolling avg and std (past 30 days)
frame30Day <- windowPartitionBy("ItemTypeCollectionKey") %>% 
  orderBy("CheckoutDate") %>% 
  rowsBetween(-30, 0)

# Add daily rolling avg and std (past 90 days)
frame90Day <- windowPartitionBy("ItemTypeCollectionKey") %>% 
  orderBy("CheckoutDate") %>% 
  rowsBetween(-90, 0)

# Add daily rolling avg and std (past 90 days)
frame365Day <- windowPartitionBy("ItemTypeCollectionKey") %>% 
  orderBy("CheckoutDate") %>% 
  rowsBetween(-365, 0)

sdfCheckoutsDaily <- sdfCheckoutsDaily %>%
  SparkR::mutate(
    CheckoutCount4DayMAvg = over(avg(sdfCheckoutsDaily$CheckoutCount), frame3Day),
    CheckoutCount4DayMStd = over(stddev_samp(sdfCheckoutsDaily$CheckoutCount), frame3Day),
    CheckoutCount15DayMAvg = over(avg(sdfCheckoutsDaily$CheckoutCount), frame15Day),
    CheckoutCount15DayMStd = over(stddev_samp(sdfCheckoutsDaily$CheckoutCount), frame15Day),
    CheckoutCount30DayMAvg = over(avg(sdfCheckoutsDaily$CheckoutCount), frame30Day),
    CheckoutCount30DayMStd = over(stddev_samp(sdfCheckoutsDaily$CheckoutCount), frame30Day),
    CheckoutCount90DayMAvg = over(avg(sdfCheckoutsDaily$CheckoutCount), frame90Day),
    CheckoutCount90DayMStd = over(stddev_samp(sdfCheckoutsDaily$CheckoutCount), frame90Day),
    CheckoutCount365DayMAvg = over(avg(sdfCheckoutsDaily$CheckoutCount), frame365Day),
    CheckoutCount365DayMStd = over(stddev_samp(sdfCheckoutsDaily$CheckoutCount), frame365Day)) %>%
  SparkR::cache()


# Feature Engineering - Group and Join ----------------------------------

# GroupBy ItemType and ItemCollection
sdfCollectionCollapsed <- sdfCollection %>%
  SparkR::groupBy(.$ItemType, .$ItemCollection) %>%
  SparkR::agg(
    BibNumCount = count(sdfCollection$BibNum),
    ItemCount = sum(sdfCollection$ItemCount),
    AvgItemCount = avg(sdfCollection$ItemCount),
    FloatingItem = sum(sdfCollection$FloatingItem),
    AvgFloatingItem = avg(sdfCollection$FloatingItem),
    DistinctTitles = approxCountDistinct(sdfCollection$Title),
    DistinctAuthors = approxCountDistinct(sdfCollection$Author),
    DistinctPublishers = approxCountDistinct(sdfCollection$Publisher)
  )

# Join with ILS to bring in more features
sdfCollectionCollapsed <- sdfCollectionCollapsed %>%
  SparkR::join(sdfIlsItemType, .$ItemType == sdfIlsItemType$ItemType, joinType = "left_outer") %>%
  SparkR::join(sdfIlsItemCollection, .$ItemCollection == sdfIlsItemCollection$ItemCollection, joinType = "left_outer") %>%
  SparkR::drop(sdfIlsItemType$ItemType) %>%
  SparkR::drop(sdfIlsItemCollection$ItemCollection) %>%
  SparkR::dropDuplicates()

# Create key - ItemTypeCollection
sdfCollectionCollapsed$ItemTypeCollectionKey <- SparkR::concat(sdfCollectionCollapsed$ItemType, lit("_"), sdfCollectionCollapsed$ItemCollection)



# Build final dataset for training * -----------------------------------------------------

# Get books with at least one checkout
sdfKeysWCheckouts <- sdfCheckoutsDaily %>% 
  SparkR::select(.$ItemTypeCollectionKey) %>% 
  SparkR::distinct()

# Join to get rest of information from sdfCollection
# Note, inner join so will drop records not listed in the collection
sdfCollectionWCheckouts <- sdfKeysWCheckouts %>%
  SparkR::join(sdfCollectionCollapsed,
               .$ItemTypeCollectionKey == sdfCollectionCollapsed$ItemTypeCollectionKey) %>%
  SparkR::drop(sdfCollectionCollapsed$ItemTypeCollectionKey)

# Cross join to get collection per Day
sdfCollectionDaily <- sdfCollectionWCheckouts %>% SparkR::crossJoin(sdfDates)

# Force writeout
write.df(sdfCollectionDaily, "_temp/collection_per_day", "parquet", "overwrite")
sdfCollectionDaily <- read.df("_temp/collection_per_day")

# Left join with checkouts
sdfDailyAll <- sdfCollectionDaily %>% 
  SparkR::join(sdfCheckoutsDaily, 
               .$ItemTypeCollectionKey == sdfCheckoutsDaily$ItemTypeCollectionKey
               & .$CheckoutDate == sdfCheckoutsDaily$CheckoutDate, 
               "left_outer") %>%
  SparkR::drop(sdfCheckoutsDaily$ItemTypeCollectionKey) %>%
  SparkR::drop(sdfCheckoutsDaily$CheckoutDate)

# Fill nas in numeric
sdfDailyAll <- sdfDailyAll %>%
  SparkR::fillna(0, cols = c("CheckoutCount",
                             "CheckoutCount3DayMAvg", "CheckoutCount3DayMStd",
                             "CheckoutCount15DayMAvg", "CheckoutCount15DayMStd",
                             "CheckoutCount30DayMAvg", "CheckoutCount30DayMStd",
                             "CheckoutCount90DayMAvg", "CheckoutCount90DayMStd",
                             "CheckoutCount365DayMAvg", "CheckoutCount365DayMStd"))

# Select only necessary columns
sdfDailyAll <- sdfDailyAll %>%
  SparkR::select(
    .$CheckoutDate,
    .$ItemTypeCollectionKey,
    .$BibNumCount,
    .$ItemCount,
    .$AvgItemCount,
    .$FloatingItem,
    .$AvgFloatingItem,
    .$DistinctTitles,
    .$DistinctAuthors,
    .$DistinctPublishers,
    .$TypeFormatGroup,
    .$TypeFormatSubGroup,
    .$NoHold,
    .$Adult,
    .$YoungAdult,
    .$Juvenile,
    .$FormerlyNoFine,
    .$Reference,
    .$ColCategoryGroup,
    .$CheckoutCount4DayMAvg,
    .$CheckoutCount4DayMStd,
    .$CheckoutCount15DayMAvg,
    .$CheckoutCount15DayMStd,
    .$CheckoutCount30DayMAvg,
    .$CheckoutCount30DayMStd,
    .$CheckoutCount90DayMAvg,
    .$CheckoutCount90DayMStd,
    .$CheckoutCount365DayMAvg,
    .$CheckoutCount365DayMStd,
    .$CheckoutCount
  ) %>%
  SparkR::orderBy(.$CheckoutDate,
                  .$ItemTypeCollectionKey)
# Drop nulls
sdfDailyAll <- dropna(sdfDailyAll)

# Write out ---------------------------------------------------

# Write out grid
write.df(sdfDailyAll, "demo_2/checkouts_daily", "parquet", "overwrite")

