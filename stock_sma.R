library('quantmod')
library(TTR)
library(dplyr)
library(data.table)
library(sqldf)
library(tcltk)

stock_dat <-getSymbols("MSFT",auto.assign = FALSE)
stock_close<-as.data.frame(stock_dat[,4])
stock_close$date <-rownames(stock_close)
names(stock_close) <-c("close","date")

stock_close$sma50 <- SMA(stock_close$close,50)
stock_close$sma200 <- SMA(stock_close$close,200)

## function
stock_sma_dat <- function(stock_ticker)
{
  suppressWarnings(stock_dat <-getSymbols(stock_ticker,auto.assign = FALSE))
  stock_close<-as.data.frame(stock_dat[,4])
  stock_close$date <-rownames(stock_close)
  names(stock_close) <-c("close","date")
  
  stock_close$sma50 <- SMA(stock_close$close,50)
  stock_close$sma200 <- SMA(stock_close$close,200)
  stock_close$sma10 <- SMA(stock_close$close,10)
  stock_close$ema30 <- EMA(stock_close$close, 30)
  stock_close$ticker <- stock_ticker
  stock_close<-stock_close[complete.cases(stock_close),]
  
  
  return(stock_close)
}


t_sma <- function(stock_c,topMA = "sma50", bottomMA = "sma200", top_bias = 1.0)
{
  if(toupper(topMA) == "SMA50") topLine <- stock_c$sma50
  if(toupper(topMA) == "SMA200") topLine <- stock_c$sma200
  if(toupper(topMA) == "SMA10") topLine <- stock_c$sma10
  if(toupper(topMA) == "EMA30") topLine <- stock_c$ema30
  if(toupper(topMA) == "CLOSE") topLine <- stock_c$close
  
  ## add bias to the top line to avoid whipsaws
  topLine <- topLine * top_bias
  
  if(toupper(bottomMA) == "SMA50") bottomLine <- stock_c$sma50
  if(toupper(bottomMA) == "SMA200") bottomLine <- stock_c$sma200
  if(toupper(bottomMA) == "SMA10") bottomLine <- stock_c$sma10
  if(toupper(bottomMA) == "EMA30") bottomLine <- stock_c$ema30
  if(toupper(bottomMA) == "CLOSE") bottomLine <- stock_c$close
  
  test_frame <-stock_c[,c(2,7)]
  test_frame<-cbind(test_frame,topLine)
  test_frame<-cbind(test_frame,bottomLine)
  rownames(test_frame) <- 1:nrow(test_frame)
  test_frame$rowID <- rownames(test_frame)
  setDT(test_frame)
  test_frame<-test_frame[,PREV_COND := shift(topLine)>shift(bottomLine) ]
  test_frame<-test_frame[,NEXT_COND := shift(topLine,type = "lead" )>shift(bottomLine,type = "lead") ]
  
  ## get Row IDs for start dates of trend
  start_up<- as.integer(test_frame[((topLine>bottomLine) & (PREV_COND == FALSE)) | ((topLine>bottomLine) & rowID == 1) ,rowID])
   
  ## get Row IDs for end dates of trend for last day
end_up<- as.integer(test_frame[((topLine>bottomLine) & (NEXT_COND == FALSE)) | ((topLine>bottomLine) & rowID == nrow(test_frame)) ,rowID])
   
  ## get up trend start and end dates
  
  trend_table <- data.frame(stock_c[start_up,2],stock_c[end_up,2])
  
  names(trend_table) <- c("Start_Date","End_Date")
  trend_table$trend_days <- as.integer(difftime(trend_table$End_Date,trend_table$Start_Date,units = "days"))
  trend_table$start_price <- stock_c[start_up,1]
  trend_table$end_price <- stock_c[end_up,1]
  trend_table$ticker <- stock_c[1,c("ticker")]
  trend_table$trend_id <-rownames(trend_table)
  trend_table<-trend_table[trend_table$trend_days>0, ]
  trend_aggr<-sqldf("select  max(sc.close) as max_close,count(distinct sc.date) as periods, t.Start_Date, t.trend_id from stock_c sc,trend_table t where sc.date between t.Start_Date and t.End_Date group by t.Start_Date, t.trend_id")
  trend_table$max_close <- trend_aggr$max_close
  trend_table$trend_trading_days <- trend_aggr$periods
 trend_table<-trend_table[c("trend_id","ticker","Start_Date","End_Date","start_price","end_price","trend_trading_days","max_close")]
  return(trend_table)
}


## t30 is returned from t_sma
  ## SMA(10) crosses EMA(30)  and SMA(50)>SMA(200)  and close > sma(200)
stock_c <- stock_sma_dat("INTC")
t30<- t_sma(stock_c,"sma10","ema30")
t50<- t_sma(stock_c,"sma50","sma200")
t_200 <- t_sma(stock_c,"close","sma200", 1.05)

## get the possible entries

## for each close over 200 SMA trend get first EMA crossover

t1<-sqldf("select t200.*, t30.Start_Date as t30_Start_Date, t30.End_Date as t30_End_Date, t30.start_price as t30_start_price,
          (sc.sma10 > sc.ema30) as EMA_CURR
from t_200 t200
          join stock_c sc
          on t200.ticker = sc.ticker
          and t200.Start_Date = sc.date
          left join t_30 t30
          on t200.ticker = t30.ticker
          and t200.Start_Date < t30.Start_Date
          and t200.End_Date > t30.Start_Date
          ")
t1<-setDT(t1)
t1<-t1[,valRank:=rank(t30_Start_Date),by=trend_id]
t1[t1$valRank==1]

entries<-stock_c[(stock_c$date %in% t30$Start_Date) & stock_c$sma50>stock_c$sma200 & stock_c$close>stock_c$sma200 ,]
## get the exits
exit<-t50[,c(1,2,5,6)]

 ## convert to dates
exit$End_Date<-as.Date(exit$End_Date)
exit$Start_Date <-as.Date(exit$Start_Date )
entries$date <- as.Date(entries$date)

 ## set data tables and keys
setDT(exit)
setDT(entries)
setkey(exit,ticker)
setkey(entries,ticker)

## cartesian product
big<-entries[exit,allow.cartesian=TRUE]
big$days_from_entry <- as.integer(difftime(big$End_Date,big$date, units = "days"))
big<-big[big$days_from_entry>0,]

big<-big[,valRank:=rank(days_from_entry),by="date"]
big$price_change<-big$end_price-big$close

peak_close<-sqldf("select  max(sc.close) ,t.Start_Date, t.trend_id from stock_c sc,t_50 t where sc.date between t.Start_Date and t.End_Date group by t.Start_Date, t.trend_id")
