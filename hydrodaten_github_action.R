library(dplyr)
library(sf)
library(readr)
library(stringr)
# install.packages("RCurl")
# Load Current Data

rds_files <- list.files("data_hydro",full.names = T) %>% 
  str_subset("messstation",negate = TRUE)
       

rds_names <- rds_files %>% 
  str_remove("data_hydro/") %>% 
  str_remove("\\.rds")
       
current_data <- lapply(rds_files,function(x){
  readRDS(x)
})
# current_data2 <- lapply(rds_files,function(x){
#   readRDS(x)
# })
names(current_data) <- rds_names


# Retrieve New Data from Thurgis
url <- "https://ows.geo.tg.ch/geofy_access_proxy/hydrologische_messnetze?Service=WFS&Version=2.0.0&request=GetFeature&typename=ms%3Awerterhebung"

data <- read_sf(url)

data_mod <- data %>% 
  select(-c(gml_id:prid)) %>% 
  as_tibble() %>% 
  select(-msGeometry)


parameter_einheit <- data_mod %>% 
  distinct(parameter,einheit) %>% 
  mutate(
    combined = paste(parameter, einheit, sep = "_") %>%
      iconv(from = "UTF-8", to = "ASCII//TRANSLIT") %>%   # remove accents/umlauts
      tolower() %>%                                       # lowercase
      gsub("[^a-z0-9]+", "_", .) %>%                      # replace non-alphanum with _
      gsub("_+", "_", .) %>%                              # collapse multiple underscores
      gsub("^_|_$", "", .)                                # trim leading/trailing underscores
  )


hydrodaten_list <- setNames(vector("list", 12), parameter_einheit$combined)


read_csv_from_geodata <- function(row){
  data_temp <- row$csv_gesamt %>% 
    gsub("\\\\n", "\n",.) %>%  
    read.csv(text = ., sep = ";", header = FALSE, stringsAsFactors = FALSE) %>% 
    filter(!V1 %in% c("#ts_id","#rows","#Timestamp")) %>% 
    mutate(V1 = lubridate::ymd_hms(V1)) %>% 
    rename(wert = "V2",timestamp="V1") %>% 
    mutate(wert = as.numeric(wert))
  
  if (nrow(data_temp)==0){
    return(NULL)
  }
  
  list_part <- row %>% 
    select(-csv_gesamt) 
  
  data_temp <- data_temp %>% bind_cols(list_part)
  
  
  return(data_temp)
  
  
  
}

is_missing_or_null <- function(x, name) {
  is.null(x[[name]]) %||% TRUE
}

# Concatenate Data
for (i in 1:nrow(parameter_einheit)){
  data_temp <- data_mod %>% 
    filter(parameter==parameter_einheit$parameter[i]) %>% 
    filter(einheit == parameter_einheit$einheit[i])
  
  temp_list <- lapply(seq_along(data_temp$obj_id), function(k){
    row <- data_temp[k,]
    read_csv_from_geodata(row)

  }) %>% bind_rows() 
  
  if (nrow(temp_list)==0) next
  
  temp_list <- temp_list %>% 
    select(obj_id,timestamp,parameter,einheit,wert) %>% 
    mutate(obj_id=as.character(obj_id))
  
  if (!is_missing_or_null(current_data,parameter_einheit$combined[i]) ){
    current_data[[parameter_einheit$combined[i]]] <- current_data[[parameter_einheit$combined[i]]] %>% 
      anti_join(temp_list,by = c("obj_id","timestamp")) %>% 
      bind_rows(temp_list)
  } else if (is_missing_or_null(current_data,parameter_einheit$combined[i])){
    current_data[[parameter_einheit$combined[i]]] <- temp_list
  }
  
  # hydrodaten_list[[parameter_einheit$combined[i]]] <- temp_list 
  
}


# write.table(messstationen, file = "messstationen.csv", quote = T, sep = ",", dec = ".", 
#             row.names = F, na = "",fileEncoding = "utf-8")

# Daten abspeichern


# Current Data as CSV
lapply(names(current_data),function(x){
  if(is.null(x)) return(NULL)
  print(x)
  temp <- current_data[[x]] %>% 
    filter(format(timestamp, "%M") == "00",
           timestamp >= (Sys.Date() - 365)) %>% 
    select(-c(einheit,parameter))
  
  write.table(temp, file = paste0("data_hydro/",x,"_current_hour.csv"), quote = T, sep = ",", dec = ".",
              row.names = F, na = "",fileEncoding = "utf-8")
  
  RCurl::ftpUpload( paste0("data_hydro/",x,"_current_hour.csv"),
                   paste0("ftp://potyhaqi.cyon.site/hydrodaten/",x,"_current_hour.csv"),
                   userpwd = paste0("OGDTG@potyhaqi.cyon.site:",Sys.getenv("OGDTG_USERPWD")))

})

# Data as zipped csv and rds
lapply(names(current_data),function(x){
  if(is.null(x)) return(NULL)
  print(x)
  
  write_csv(current_data[[x]], paste0("data_hydro/",x,"_full.csv.gz"))
  
  RCurl::ftpUpload(paste0("data_hydro/",x,"_full.csv.gz"),
                   paste0("ftp://potyhaqi.cyon.site/hydrodaten/",x,"_full.csv.gz"),
                   userpwd = paste0("OGDTG@potyhaqi.cyon.site:",Sys.getenv("OGDTG_USERPWD")))
  
  saveRDS(current_data[[x]],paste0("data_hydro/",x,".rds"))
  
  
})


# Messstaionen
messstationen <- data %>% select(-c(gml_id:prid,csv_gesamt))

messstationen$msGeometry %>% st_transform( 4326)

messstationen <- messstationen %>%
  mutate(
    geom_wgs84 = st_transform(msGeometry, 4326),                 # transform to WGS84
    coords = st_coordinates(geom_wgs84),                         # extract coords as matrix
    lon = coords[, 1],                                           # first col = X = lon
    lat = coords[, 2]                                            # second col = Y = lat
  ) %>%
  select(-coords,geom_wgs84,msGeometry)  # optional, remove helper column

write.table(messstationen, file = paste0("data_hydro/messstationen.csv"), quote = T, sep = ",", dec = ".",
            row.names = F, na = "",fileEncoding = "utf-8")
RCurl::ftpUpload( "data_hydro/messstationen.csv",
                  "ftp://potyhaqi.cyon.site/hydrodaten/messstationen.csv",
                  userpwd = paste0("OGDTG@potyhaqi.cyon.site:",Sys.getenv("OGDTG_USERPWD")))
saveRDS(messstationen,"data_hydro/messstationen.rds")






