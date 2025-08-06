library(tidyverse)
library(httr)

library(jsonlite)


# data <- fromJSON("https://www.hydrodaten.tg.ch/data/internet/layers/20/index.json")



url_einheit <- "https://daten.ostluft.ch/index.php?id=datenabfragen&type=72&tx_ostluft_datenabfragen[messstationId]=f64bdd31-226e-4d19-b7b2-2c6bd958df8c&tx_ostluft_datenabfragen[action]=listMessgroesse&tx_ostluft_datenabfragen[controller]=Plugin%5CDatenabfragenApi"



get_messstandorte <- function(){
  url <- "https://daten.ostluft.ch/index.php?id=datenabfragen&type=72&tx_ostluft_datenabfragen[action]=listOstluftMessstandorte&tx_ostluft_datenabfragen[controller]=Plugin%5CDatenabfragenApi"
  
  res <- httr::GET(url)
  
  content_list <- httr::content(res)
  
  content_list$items %>% dplyr::bind_rows()
  
}

get_messgroessen <- function(messstationId){
  
  url <- glue::glue("https://daten.ostluft.ch/index.php?id=datenabfragen&type=72&tx_ostluft_datenabfragen[messstationId]={messstationId}&tx_ostluft_datenabfragen[action]=listMessgroesse&tx_ostluft_datenabfragen[controller]=Plugin%5CDatenabfragenApi")
  
  res <- httr::GET(url)
  content_list <- content(res)
  
  available_messgroessen <- lapply(content_list$object,function(x){
    bind_rows(x)
  })
  
  available_messgroessen$messgroesse <- available_messgroessen$messgroesse %>% 
    select(messgroesseId,name)
  
  return(available_messgroessen)
}


get_zeitfenster <- function(messgroesseId){
  url <- glue::glue("https://daten.ostluft.ch/index.php?id=datenabfragen&type=72&tx_ostluft_datenabfragen[messgroesseId]={messgroesseId}&tx_ostluft_datenabfragen[action]=listZeitfenster&tx_ostluft_datenabfragen[controller]=Plugin%5CDatenabfragenApi")
  
  res <- httr::GET(url)
  content_list <- httr::content(res)
  
  available_zeitfenster <- lapply(content_list$object,function(x){
    dplyr::bind_rows(x)
  })
  
  available_zeitfenster$zeitfenster <- available_zeitfenster$zeitfenster %>% 
    dplyr::select(zeitfensterId ,titel,dauereinheit ,dauerzeit ,name,offseteinheit,wiederholungeinheit ,wiederholungzeit )
  
  return(available_zeitfenster)
}

get_kanal <- function(messstationId,messgroesseId,zeitfensterId){
  
  messstationId <- paste0(messstationId,collapse = "%2C")
  messgroesseId <- paste0(messgroesseId,collapse = "%2C")
  zeitfensterId <- paste0(zeitfensterId,collapse = "%2C")
  
  url <- glue::glue("https://daten.ostluft.ch/index.php?id=datenabfragen&type=72&tx_ostluft_datenabfragen[messstationId]={messstationId}&tx_ostluft_datenabfragen[messgroesseId]={messgroesseId}&tx_ostluft_datenabfragen[zeitfensterId]={zeitfensterId}&tx_ostluft_datenabfragen[action]=listKanal&tx_ostluft_datenabfragen[controller]=Plugin%5CDatenabfragenApi")
  
  res <- httr::GET(url)
  content_list <- httr::content(res)
  
  if (length(content_list$items)==0){
    stop("Keine Daten")
  }
  
  content_list$items %>% dplyr::bind_rows(.id = "kanalId")
}



prepare_form_data <- function(kanal_data) {
  form_data <- list()
  
  for (i in seq_len(nrow(kanal_data))) {
    kanal_id <- kanal_data$kanalId[i]
    
    for (key in names(kanal_data)) {
      value <- kanal_data[[key]][i]
      full_key <- sprintf("kanal[%s][%s]", kanal_id, key)
      form_data[[full_key]] <- as.character(value)
    }
  }
  
  return(form_data)
}



prepare_query_params <- function(days_from_today){
  
  query_params <- list(
    id = "datenabfragen",
    type = "72",
    `tx_ostluft_datenabfragen[action]` = "storeKanalConfiguration",
    `tx_ostluft_datenabfragen[controller]` = "Plugin\\DatenabfragenApi",
    `tx_ostluft_datenabfragen[type]` = "csv",
    `tx_ostluft_datenabfragen[windgrafik]` = "0",
    `tx_ostluft_datenabfragen[windgrafik_zeitfenster]` = "h",
    `tx_ostluft_datenabfragen[zeitmodus]` = "relative",
    `tx_ostluft_datenabfragen[selektion][ankerzeit]` = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    `tx_ostluft_datenabfragen[selektion][dauer][wert]` = paste0("-",as.character(days_from_today)),
    `tx_ostluft_datenabfragen[selektion][dauer][einheit]` = "d",
    `tx_ostluft_datenabfragen[selektion][offset][wert]` = "1",
    `tx_ostluft_datenabfragen[selektion][offset][einheit]` = "d"
  )
  
  query_params
  
}


get_fhash <- function(kanal_data, days_from_today){
  form_data <- prepare_form_data(kanal_data)
  query_params <- prepare_query_params(days_from_today)
  
  # ---- Custom headers ----
  headers <- add_headers(
    `Accept` = "*/*",
    `Content-Type` = "application/x-www-form-urlencoded; charset=UTF-8",
    `Origin` = "https://daten.ostluft.ch",
    `Referer` = "https://daten.ostluft.ch/",
    `User-Agent` = "Mozilla/5.0",
    `X-Requested-With` = "XMLHttpRequest"
  )
  
  # ---- Make the request ----
  response <- POST(
    url = "https://daten.ostluft.ch/index.php",
    query = query_params,
    body = form_data,
    encode = "form",
    headers
  )
  
  httr::content(response)$object
}



download_ostluft_csv <- function(fhash){
  url <- glue::glue("https://daten.ostluft.ch/index.php?id=datenabfragen&type=72&tx_ostluft_datenabfragen[action]=loadCsv&tx_ostluft_datenabfragen[controller]=Plugin%5CDatenabfragenApi&tx_ostluft_datenabfragen[fhash]={fhash}")
  
  res <- httr::GET(url)
  
  # Step 2: Extract text content with correct encoding
  csv_text <- content(res, "text", encoding = "ISO-8859-1")
  
  col_names <- read.csv(text = csv_text, nrows = 3, sep = ";", dec = ".", stringsAsFactors = FALSE,header = F)[1:4,]
  
  col_names_full <- apply(col_names[1:3, ], 2, function(x) paste(x, collapse = "_"))
  
  
  # Step 3: Parse CSV starting from row 5 (skip metadata), semicolon sep, dot decimal
  data <- read.csv(text = csv_text, skip = 3, sep = ";", dec = ".", stringsAsFactors = FALSE)
  data <- data[1:(nrow(data) - 2), ]
  names(data) <- col_names_full
  data
  
}


get_ostluft_data <- function(messstationId,messgroesseId,zeitfensterId, days_from_today){
  
  kanal_data <- get_kanal(messstationId,messgroesseId,zeitfensterId)
  
  fhash <- get_fhash(kanal_data,days_from_today)
  
  download_ostluft_csv(fhash)
  
}



messstationen_tg <- c("Amriswil, Alleestrasse","Amriswil, Hudelmoos Nr 0","Amriswil, Hudelmoos Nr 3","Arbon, Altstadtumfahrung","Arbon, Bahnhofstrasse","Arbon, Ev. Kirche","Arbon, Mole","Arbon, Sonnenhügelstrasse","Arbon, Stadthaus","Bürglen, Rossweid","Bürglen, Wiide","Diessenhofen, Franzosenstrasse","Egnach, Buech","Egnach, Siebeneichen","Eschenz, Alte Bahnhofstrasse","Frauenfeld, Bahnhofstrasse","Frauenfeld, Kurzdorf","Frauenfeld, Rathaus","Frauenfeld, Sand","Konstanz, Wallgutstrasse","Kreuzlingen, Hafenstrasse 3","Kreuzlingen, Konradstrasse","Kreuzlingen, Konstanzerstrasse 5","	
Kreuzlingen, Löwenstrasse","Kreuzlingen, Marktweg","Kreuzlingen, Romanshornerstrasse","Kreuzlingen, Weinberg","Langrickenbach, Grabewis","Lengwil, Weiher Nr 1","Märstetten, ARA","Roggwil, Gries","Romanshorn, Arbonerstrasse","Romanshorn, Bahnhof","Romanshorn, Florastrasse","Romanshorn, Kreuzlingerstrasse","Wängi, Froberg","Wängi, Weiertal","Weinfelden, Berufsbildungszentrum","Weinfelden, Deucherstrasse","Weinfelden, Nollenstrasse","Weinfelden, Weid")


messtandorte <- get_messstandorte()

messtandorte_tg <-  messtandorte %>% 
  filter(nameLang %in% messstationen_tg)



stationen_tg <- messtandorte_tg %>% 
  filter(art=="messstation") %>% 
  pull(messstationId)


messgroessen_av <- get_messgroessen(paste0(stationen_tg,collapse = "%2C"))

zeitfenster_av <- get_zeitfenster(paste0(messgroessen_av$messgroesse$messgroesseId,collapse = "%2C"))

tageswert <- "b5477cd2-2dda-49cf-9585-1cf046cf65c4"
jahreswert <- "1271eebe-a7b3-44af-9a3a-4882c9801b2f"
stundenwert <- "4b3de58e-4fe0-4402-aee8-b1782aa8e49a"
monatswert <- "23a8a6e8-10bc-470b-97c2-65c67dc8f6fe"



# Tageswerte
av_messgroessen <- messgroessen_av$messgroesse$messgroesseId %>% unique()


av_time <- zeitfenster_av$zeitfenster %>% distinct()

tg_data_day <- list()

for (mg in av_messgroessen){
  mg_name <- messgroessen_av$messgroesse$name[which(mg == messgroessen_av$messgroesse$messgroesseId)]
  print(mg_name)
  tg_data_day[[mg_name]] <- tryCatch({
    df <- get_ostluft_data(messstationId = stationen_tg,messgroesseId = mg,zeitfensterId = tageswert,days_from_today = 60000)
    df[, colSums(!is.na(df)) > 0]
    
  },error = function(cond){
    print(cond)
    NULL
  })
  Sys.sleep(1)
}


tg_data_hour <- list()

for (mg in av_messgroessen){
  mg_name <- messgroessen_av$messgroesse$name[which(mg == messgroessen_av$messgroesse$messgroesseId)]
  print(mg_name)
  tg_data_hour[[mg_name]] <- tryCatch({
    df <- get_ostluft_data(messstationId = stationen_tg,messgroesseId = mg,zeitfensterId = stundenwert,days_from_today = 60000)
    df[, colSums(!is.na(df)) > 0]
    
  },error = function(cond){
    print(cond)
    
    NULL
  })
  Sys.sleep(1)
}


tg_data_month <- list()

for (mg in av_messgroessen){
  mg_name <- messgroessen_av$messgroesse$name[which(mg == messgroessen_av$messgroesse$messgroesseId)]
  print(mg_name)
  tg_data_month[[mg_name]] <- tryCatch({
    df <- get_ostluft_data(messstationId = stationen_tg,messgroesseId = mg,zeitfensterId = monatswert,days_from_today = 60000)
    df[, colSums(!is.na(df)) > 0]
    
  },error = function(cond){
    print(cond)
    
    NULL
  })
  Sys.sleep(1)
}



ostluft_data_full <- list()


for (i in 6:nrow(av_time)){
  ostluft_data_full[[av_time$name[i]]] <- list()
  print(av_time$name[i])
  for (mg in av_messgroessen){
    mg_name <- messgroessen_av$messgroesse$name[which(mg == messgroessen_av$messgroesse$messgroesseId)]
    print(mg_name)
    
    ostluft_data_full[[av_time$name[[i]]]][[mg_name]] <- tryCatch({
      df <- get_ostluft_data(messstationId = stationen_tg,messgroesseId = mg,zeitfensterId = av_time$zeitfensterId[i],days_from_today = 60000)
      df[, colSums(!is.na(df)) > 0]
      
    },error = function(cond){
      tryCatch({
        df <- get_ostluft_data(messstationId = stationen_tg,messgroesseId = mg,zeitfensterId = av_time$zeitfensterId[i],days_from_today = 2000)
        df[, colSums(!is.na(df)) > 0]
        
      },error = function(cond){
        print(cond)
        
        NULL
      })
    })
  }
}

for (name in names(ostluft_data_full)){
  temp_name <- name %>% 
    str_remove_all("-|\\,|\\.")
  
  saveRDS(ostluft_data_full[[name]],paste0("\\\\svmktgaccp01.tg.ch\\data$\\SK-Daten-und-Prozesse\\stat\\ogd\\ostluft\\",temp_name,".rds"))
}
names(ostluft_data_full) <- names(ostluft_data_full) %>% 
  str_remove_all("-|\\,|\\.")
ostluft_data_full %>% View()
# names(ostluft_data_full) %>% 
#   str_remove_all("-|\\,|\\.")
# 
# tg_data_day <- get_ostluft_data(messstationId = stationen_tg,messgroesseId = messgroessen_av$messgroesse$messgroesseId[5],zeitfensterId = tageswert,days_from_today = 10)
# saveRDS(tg_data_day,"\\\\svmktgaccp01.tg.ch\\data$\\SK-Daten-und-Prozesse\\stat\\ogd\\ostluft\\tg_data_day.rds")
# 
# 
# saveRDS(tg_data_hour,"\\\\svmktgaccp01.tg.ch\\data$\\SK-Daten-und-Prozesse\\stat\\ogd\\ostluft\\tg_data_hour.rds")
# tg_data_day$Lufttemperatur %>% View()
# 
# 
# tg_data_day <- readRDS("\\\\svmktgaccp01.tg.ch\\data$\\SK-Daten-und-Prozesse\\stat\\ogd\\ostluft\\tg_data_day.rds")
