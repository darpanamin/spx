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
  stock_close<-as.data.frame(stock_dat[,c(4,5)])
  stock_close$date <-rownames(stock_close)
  names(stock_close) <-c("close","volume","date")
  if (nrow(stock_close)<200){stock_close$sma200<-NA; return(stock_close[complete.cases(stock_close),]);}
  
  if( (nrow(stock_close)>50)) {stock_close$sma50 <-SMA(stock_close$close,50)} else {stock_close$sma50 <-NA}
  
   if( (nrow(stock_close)>200)) {stock_close$sma200 <-SMA(stock_close$close,200)} else {stock_close$sma200 <-NA}
          
  stock_close$sma10 <- SMA(stock_close$close,10)
  stock_close$ema30 <- EMA(stock_close$close, 30)
  stock_close$ticker <- stock_ticker
  
  stock_close$vol20 <- SMA(stock_close$volume,20)
  stock_close$vol50 <- SMA(stock_close$volume,50)
  if( (nrow(stock_close)>200)) {stock_close$vol200 <- SMA(stock_close$volume,200)} else {stock_close$vol200 <-NA}
  
  
  
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
  
   # 2 = date 7 = ticker
  test_frame <-stock_c[,c(3,8)]
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
  
  trend_table <- data.frame(stock_c[start_up,3],stock_c[end_up,3])
  
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
t_200_ema_lace <- function (stock_c, t200,t30)
{
t1<-sqldf("select t200.*, t30.Start_Date as t30_Start_Date, t30.End_Date as t30_End_Date, t30.start_price as t30_start_price,
          (sc.sma10 > sc.ema30) as EMA_CUR, (sc.sma50 > sc.sma200) as SMA50_200_CUR, (sc.close > sc.sma50) as CLOSE_50_CUR
from t200 t200
          join stock_c sc
          on t200.ticker = sc.ticker
          and t200.Start_Date = sc.date
          left join t30 t30
          on t200.ticker = t30.ticker
          and t200.Start_Date < t30.Start_Date
          and t200.End_Date > t30.Start_Date
          ")
t1<-setDT(t1)
t1<-t1[,valRank:=rank(t30_Start_Date),by=trend_id]
t1<-t1[t1$valRank==1]
return (t1)
}

aggregate( 100*((t2$end_price-t2$start_price)/t2$start_price), list(Cont_SMA = t2$CLOSE_50_CUR), mean)

t_quick_200 <- function(ticker)
{
  f_stock_c<-stock_sma_dat(ticker)
  f_t200<-t_sma(f_stock_c,"close","sma200",1.04)
  f_t30<-t_sma(f_stock_c,"sma10","ema30")
  
  t2<-t_200_ema_lace(f_stock_c,f_t200,f_t30)
 
  ##fa<- aggregate( 100*((t2$end_price-t2$start_price)/t2$start_price), list(CLOSE_SMA50 = t2$CLOSE_50_CUR), mean)
 
fa<- sqldf("select t2.CLOSE_50_CUR, avg(100*((t2.end_price-t2.start_price)/t2.start_price)) as START_END, 
 avg(100*((t2.max_close-t2.start_price)/t2.start_price)) as START_MAX
  from t2 t2
group by t2.CLOSE_50_CUR")

 names(fa) <- c("CLOSE_SMA50","PCT_Chng_Start_End","PCT_Chng_Start_Max") 
 return(fa)
}

t_slope <- function(ticker)
{
  stock_c <- stock_c <-stock_sma_dat(ticker)
  if (nrow(stock_c)==0) {return()}
 ts50<- ((stock_c[nrow(stock_c),c("sma50")] - stock_c[(nrow(stock_c)-35),c("sma50")])/35)/stock_c[(nrow(stock_c)-35),c("sma50")]
 ts200<- ((stock_c[nrow(stock_c),c("sma200")] - stock_c[(nrow(stock_c)-35),c("sma200")])/35)/stock_c[(nrow(stock_c)-35),c("sma200")]
 ts10<- ((stock_c[nrow(stock_c),c("sma10")] - stock_c[(nrow(stock_c)-35),c("sma10")])/35)/stock_c[(nrow(stock_c)-35),c("sma10")]
 
 vs20<- ((stock_c[nrow(stock_c),c("vol20")] - stock_c[(nrow(stock_c)-35),c("vol20")])/35)/stock_c[(nrow(stock_c)-35),c("vol20")]
 vs50<- ((stock_c[nrow(stock_c),c("vol50")] - stock_c[(nrow(stock_c)-35),c("vol50")])/35)/stock_c[(nrow(stock_c)-35),c("vol50")]
 vs200<- ((stock_c[nrow(stock_c),c("vol200")] - stock_c[(nrow(stock_c)-35),c("vol200")])/35)/stock_c[(nrow(stock_c)-35),c("vol200")]
 
 slope_table <- data.frame(ticker,100*ts10,100*ts50,100*ts200,100*vs20,100*vs50,100*vs200)
 names(slope_table)<-c("ticker","ts10","ts50","ts200","vs20","vs50","vs200")
 return(slope_table)
}


t_slope_sum <-function(stock_ticker_list)
{
  j<-do.call("rbind", lapply(stock_ticker_list,t_slope)) 
  j[j$ts50>0|j$ts10>0|j$ts200>0,]
  return(j)
}

j<-t_slope_sum(stocksp[1:299,c("Symbol")])
j[j$vs50>0 & j$ts200>0,]

y<-sqldf("select 100*((t2.end_price-t2.start_price)/t2.start_price) as START_END, 
 100*((t2.max_close-t2.start_price)/t2.start_price) as START_MAX, t2.CLOSE_50_CUR
  from t2 t2
group by CLOSE_50_CUR")


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

t_sma(stock_sma_dat("TGT"),"close","sma200",1.02)