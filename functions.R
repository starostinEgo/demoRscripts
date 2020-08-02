library(AzureRMR)
library(AzureStor)
library(tidyverse)
library(data.table)
library(lubridate)
library(arrow)
library(forecast)
library(doParallel)
library(xts)


##create sas for blob container
cont <- blob_container("https://demodatawestus.blob.core.windows.net/bigdata/"
                       ,sas = "?sv=2019-12-12&ss=b&srt=co&sp=rlx&se=2025-08-01T11:05:06Z&st=2020-08-01T03:05:06Z&spr=https,http&sig=RRUJBvgst4fofIxINny6R9W49MJLifpdvHrLq6PR%2FSE%3D")


loadDataFromBlobContainer <- function(pathFolder){
  
  pathFolder <- paste0(pathFolder,"/data/")
  source <- "demodata2/"
  
  ##load retailAssort
  nameLoadData <- "assortRetail"
  sourceLoad <- paste0(source,nameLoadData,".parquet/","*.parquet")
  
  if (file.exists(paste0(pathFolder,nameLoadData))) {unlink(paste0(pathFolder,nameLoadData),recursive = T)}
  storage_multidownload(cont,src = sourceLoad,dest = paste0(pathFolder,nameLoadData))
  
  ##load check
  nameLoadData <- "check"
  sourceLoad <- paste0(source,nameLoadData,".parquet/","*.parquet")
  
  if (file.exists(paste0(pathFolder,nameLoadData))) {unlink(paste0(pathFolder,nameLoadData),recursive = T)}
  storage_multidownload(cont,src = sourceLoad,dest = paste0(pathFolder,nameLoadData))
  
  ##load stock
  nameLoadData <- "stock"
  sourceLoad <- paste0(source,nameLoadData,".parquet/","*.parquet")
  
  if (file.exists(paste0(pathFolder,nameLoadData))) {unlink(paste0(pathFolder,nameLoadData),recursive = T)}
  storage_multidownload(cont,src = sourceLoad,dest = paste0(pathFolder,nameLoadData))
  
  ##load shop
  nameLoadData <- "shop"
  sourceLoad <- paste0(source,nameLoadData,".parquet/","*.parquet")
  
  if (file.exists(paste0(pathFolder,nameLoadData))) {unlink(paste0(pathFolder,nameLoadData),recursive = T)}
  storage_multidownload(cont,src = sourceLoad,dest = paste0(pathFolder,nameLoadData))
  
  
}

row.sales <- function(pathFolder){
  
  data <- list.files(path = paste0(pathFolder,"check")
                       ,pattern = "*.parquet"
                       ,full.names = T) %>%
            map_df(~read_parquet(.))
  data <- data.table(data)
  
  data[,.(sales = sum(sales),salesRub = sum(salesRub)),.(shopId,skuId,date)]
}

row.stock <- function(pathFolder){
  data <- list.files(path = paste0(pathFolder,"stock")
                     ,pattern = "*.parquet"
                     ,full.names = T) %>%
    map_df(~read_parquet(.))
  data <- data.table(data)
  
}

row.assort <- function(pathFolder){
  data <- list.files(path = paste0(pathFolder,"assortRetail")
                     ,pattern = "*.parquet"
                     ,full.names = T) %>%
    map_df(~read_parquet(.))
  data <- data.table(data)
  
  shop <- list.files(path = paste0(pathFolder,"shop")
                     ,pattern = "*.parquet"
                     ,full.names = T) %>%
    map_df(~read_parquet(.))
  shop <- data.table(shop)
  
  ## create table - shopId-skuId-date-status
  minDate <- ymd('2015-01-01')
  maxDate <- ymd('2020-06-01')
  calendar <- data.table(seq(minDate,maxDate,by = "day"))
  names(calendar) <- c("date")
  
  data <- tidyr::crossing(data,calendar)
  data <- data.table(data)
  data <- data[startDate <= date & finishDate >= date]
  data <- data[status == 1]
  
  ##merge shop and assort active
  setkey(data,storeId)
  setkey(shop,storeId)
  data <- merge(data,shop)
  
  return(data[,.(shopId,skuId,date)])
  
  
}

createOlapCube <- function(pathFolder){
  
  ## fixing data imetation data
  nowDate <- ymd('2020-02-10')
  
  #create calendar
  minDate <- nowDate - years(3)
  calendar <- data.table(seq(minDate,nowDate,by = "day"))
  names(calendar) <- c("date")
  
  stock <- row.stock(pathFolder)
  minMaxDataStock <- stock[,.(minDate = min(date),maxDate = max(date)),.(shopId,skuId)]
  sales <- row.sales(pathFolder)
  assort <- row.assort(pathFolder)
  assort$st <- 1
  
  shopId <-stock[date >= minDate,.N,shopId][,shopId]
  skuId <-stock[date >= minDate,.N,skuId][,skuId]
  
  data <- tidyr::crossing(shopId,calendar)
  data <- data.table(data)
  data <- tidyr::crossing(data,skuId)
  data <- data.table(data)
  
  ## merge all data in olap cube
  setkey(data,shopId,skuId,date)
  setkey(stock,shopId,skuId,date)
  setkey(assort,shopId,skuId,date)
  setkey(sales,shopId,skuId,date)
  setkey(minMaxDataStock,shopId,skuId)
  
  data <- merge(data,stock,all.x = T)
  data[is.na(stock)]$stock <- 0
  data <- merge(data,sales,all.x = T)
  data[is.na(sales)]$sales <- 0
  data[is.na(salesRub)]$salesRub <- 0
  data <- merge(data,assort,all.x = T)
  data[is.na(st)]$st <- 0
  setkey(data,shopId,skuId)
  data <- merge(data,minMaxDataStock,all.x = T)
  data <- data[date >= minDate & date <= maxDate]
  data[,minDate:=NULL]
  data[,maxDate:=NULL]
  
  return(data)
}

preProcessTimeSeries <- function(data){
  
  ## only active assort
  nowDate <-data[,max(date)]
  shopIdSkuId <- data[date == nowDate & st == 1,.N,.(shopId,skuId)][,.(shopId,skuId)]
  setkey(shopIdSkuId,shopId,skuId)
  setkey(data,shopId,skuId)
  data <- merge(data,shopIdSkuId)
  
  ## change mean sales if stock = 0
  meanSales <- data[,.(meanSales = mean(sales),meanSalesRub = mean(salesRub)),.(shopId,skuId)]
  setkey(data,shopId,skuId)
  setkey(meanSales,shopId,skuId)
  data <- merge(data,meanSales)
  data[stock <=0 & sales <=0]$salesRub <-  data[stock <=0 & sales <=0]$meanSalesRub  
  data[stock <=0 & sales <=0]$sales <-  data[stock <=0 & sales <=0]$meanSales
    
  data[,meanSales:=NULL]
  data[,meanSalesRub:=NULL]
  
  data
  
}

fs.means <- function(ts,step){
  a <- mean(ts)
  rep(a,step)
}

fs.ses <- function(ts,step){
  a <- ses(ts,h=step)
  predict(a,n.ahead=step)$mean
}

fs.stlEts <-function(ts,step){
  a <- try(stl(ts, s.window="periodic"))
  b <- try(forecast(a, h = step, method = 'ets', ic = 'bic', opt.crit='mae'),silent = T)
  predict(b,n.ahead=step)$mean
}

fs.snaive <- function(ts,step){
  a <- snaive(ts, h = step)
  predict(a,n.ahead=step)$mean
}

fs.naive <- function(ts,step){
  a <- naive(ts, h = step)
  predict(a,n.ahead=step)$mean
}

fs.holtWinterAdditive <- function(ts,step){
  a<-try(HoltWinters(ts, seasonal="additive"),silent = T)
  try(predict(a,n.ahead=step),silent = T)
}

fs.holtWinterMult <- function(ts,step){
  a<-try(HoltWinters(ts, seasonal="multiplicative"),silent = T)
  try(predict(a,n.ahead=step),silent = T)
}

fs.holt <- function(ts,step){
  a <- holt(ts,h=step)
  predict(a,n.ahead=step)$mean
}

fs.croston <- function(ts,step){
  a<-try(croston(ts,h=step,alpha=0.2),silent = T)
  try(predict(a,n.ahead=step)$mean,silent = T)
}

fs.nnet <- function(ts,step){
  a<-try(nnetar(ts),silent = T)
  try(forecast(a,h=step)$mean,silent = T)
}

fs.tbats <-function(ts,step){
  a<-try(tbats(ts),silent = T)
  try(forecast(a,h=step)$mean,silent = T)
}

fs.arima <- function(ts,step){
  a<-try(auto.arima(ts),silent = T)
  try(forecast(a,h=step)$mean,silent = T)
}

forecastOneShopIdSkuId <- function(data,shopIdSkuId){
  
  step <- 8
  
  setkey(data,shopId,skuId)
  setkey(shopIdSkuId,shopId,skuId)
  data <- merge(data,shopIdSkuId)
  
  data <- data[order(date)]
  ts <- ts(data$sales,frequency = 365)
  
  result <- data.table("fs.means",t(fs.means(ts,step)))
  result <- rbind(result,data.table("fs.ses",t(fs.ses(ts,step)))) 
  try(result <- rbind(result,data.table("fs.arima",t(fs.arima(ts,step)))))
  try(result <- rbind(result,data.table("fs.croston",t(fs.croston(ts,step))))) 
  try(result <- rbind(result,data.table("fs.holt",t(fs.holt(ts,step)))))
  try(result <- rbind(result,data.table("fs.holtWinterAdditive",t(fs.holtWinterAdditive(ts,step))))) 
  try(result <- rbind(result,data.table("fs.holtWinterMult",t(fs.holtWinterMult(ts,step)))))
  result <- rbind(result,data.table("fs.naive",t(fs.naive(ts,step)))) 
  try(result <- rbind(result,data.table("fs.nnet",t(fs.nnet(ts,step)))))
  try(result <- rbind(result,data.table("fs.snaive",t(fs.snaive(ts,step)))))
  try(result <- rbind(result,data.table("fs.stlEts",t(fs.stlEts(ts,step)))))
  try(result <- rbind(result,data.table("fs.tbst",t(fs.tbats(ts,step)))))
  
  result <- cbind(result,shopIdSkuId)
  names(result)[1] <- c("method")
  reorder <- c("shopId","skuId","method",paste0("V",seq(1:step)))
  setcolorder(result,reorder)
  
  return(result)
}




