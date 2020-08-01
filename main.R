source("importData.R")

## load data from blob container to localFolder
## use getwd() - local path, if you want change
## make path for Folder
#loadDataFromBlobContainer(getwd())

## create olap cube for timeseries
## path for local Folder where data
data <- createOlapCube(paste0(getwd(),"/data/"))


## preprocess
## use information about actual assort and
## stock change data for timesSeries
data <- preProcessTimeSeries(data)





