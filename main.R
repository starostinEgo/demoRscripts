source("functions.R")

## load data from blob container to localFolder
## use getwd() - local path, if you want change
## make path for Folder
st<-Sys.time()
#loadDataFromBlobContainer(getwd())
print("загрузка данных выполнена за: ")
print(Sys.time() - st)

## create olap cube for timeseries
## path for local Folder where data
st<-Sys.time()

data <- createOlapCube(paste0(getwd(),"/data/"))

print("создание куба выполнено за: ")
print(Sys.time() - st)


## preprocess
## use information about actual assort and
## stock change data for timesSeries
st<-Sys.time()

data <- preProcessTimeSeries(data)

print("процесс очистки выполнен за: ")
print(Sys.time() - st)


## use parallel run timeseries on shopId - skuId
st<-Sys.time()
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
fwrite(forecastDT,"forecastDT.csv")
print("прогнозирование выполнен за: ")
print(Sys.time() - st)




