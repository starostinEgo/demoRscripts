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


## use parallel run timeseries on shopId - skuId
nCores <- detectCores() - 1
cl <- makeCluster(nCores)
registerDoParallel(cl)

##length data for cores
shopIdSkuId <- data[,.N,.(shopId,skuId)][,.(shopId,skuId)]
l <- dim(shopIdSkuId)[1]
result <- foreach(i = 1:l,.packages = c("data.table","forecast")) %dopar% 
  forecastOneShopIdSkuId(data,shopIdSkuId[i])

stopCluster(cl)

## rbind result forecast after parallels
forecastDT <- rbindlist(result)





