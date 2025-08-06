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


prepare_as_tidy_data <- function(data,element_name,messstandorte){
  if (!is.data.frame(data)){
    return(NULL)
  }
  data %>% 
    mutate_all(as.character) %>% 
    pivot_longer(3:last_col(), names_to = "messstelle", values_to = "messwert") %>% 
    mutate(messstelle = str_replace(messstelle, "_.*", "")) %>% 
    left_join(messstandorte, join_by(messstelle == nameLang)) %>% 
    filter(messwert != "") %>% 
    filter(!is.na(messwert)) %>% 
    select(-art) %>% 
    rename(startzeit = "Startzeit__",
           endzeit = "Endzeit__") %>% 
    mutate(indikator = element_name) %>% 
    mutate(startzeit = lubridate::dmy_hm(startzeit),
           endzeit = lubridate::dmy_hm(endzeit))
}


clean_string <- function(text) {
  text %>%
    str_replace_all("\\s+", "_") %>%               # Replace whitespace with underscore
    str_replace_all("[^a-zA-Z0-9_]", "") %>%       # Remove all except letters, numbers, and underscore
    str_to_lower()                                 # Convert to lowercase
}


save_data_by_year <- function(data,data_name, datetime_col = "startzeit", output_dir = ".",upload = TRUE,max_year = NULL) {
  # Ensure datetime column is in POSIXct format
  data <- data %>%
    mutate(year = year(.data[[datetime_col]]))
  
  if (!is.null(max_year)){
    data <- data %>% 
      filter(year>=max_year)
  }
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Split by year and write each to CSV.GZ
  data %>%
    group_split(year) %>%
    purrr::walk(~ {
      yr <- unique(.x$year)
      filename <- file.path(output_dir, paste0(data_name,"_", yr, ".csv.gz"))
      
      write_csv(.x %>% select(-year), filename)
      if (upload){
        RCurl::ftpUpload(filename,
                         paste0("ftp://potyhaqi.cyon.site/ostluft/",data_name,"_", yr, ".csv.gz"),
                         userpwd = paste0("OGDTG@potyhaqi.cyon.site:",Sys.getenv("OGDTG_USERPWD"))
                         
        )
      }
    })
  
  
}


save_last_365_days <- function(data, data_name, datetime_col = "startzeit", output_dir = ".") {
  # Ensure datetime column is in POSIXct format
  data <- data %>%
    mutate(.datetime = as.POSIXct(.data[[datetime_col]])) %>%
    filter(.datetime >= Sys.Date() - 365)
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Construct file path and save CSV
  filename <- file.path(output_dir, paste0(data_name, "_current.csv"))
  write.table(data %>% select(-.datetime), file = filename, quote = T, sep = ",", dec = ".", 
              row.names = F, na = "",fileEncoding = "utf-8")
}