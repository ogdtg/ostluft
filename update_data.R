# Update data 
install.packages("RCurl")

source("ostluft_functions.R")


# Thurgauer Messstandorte definieren

messstationen_tg <- c("Amriswil, Alleestrasse","Amriswil, Hudelmoos Nr 0","Amriswil, Hudelmoos Nr 3","Arbon, Altstadtumfahrung","Arbon, Bahnhofstrasse","Arbon, Ev. Kirche","Arbon, Mole","Arbon, Sonnenhügelstrasse","Arbon, Stadthaus","Bürglen, Rossweid","Bürglen, Wiide","Diessenhofen, Franzosenstrasse","Egnach, Buech","Egnach, Siebeneichen","Eschenz, Alte Bahnhofstrasse","Frauenfeld, Bahnhofstrasse","Frauenfeld, Kurzdorf","Frauenfeld, Rathaus","Frauenfeld, Sand","Konstanz, Wallgutstrasse","Kreuzlingen, Hafenstrasse 3","Kreuzlingen, Konradstrasse","Kreuzlingen, Konstanzerstrasse 5","	
Kreuzlingen, Löwenstrasse","Kreuzlingen, Marktweg","Kreuzlingen, Romanshornerstrasse","Kreuzlingen, Weinberg","Langrickenbach, Grabewis","Lengwil, Weiher Nr 1","Märstetten, ARA","Roggwil, Gries","Romanshorn, Arbonerstrasse","Romanshorn, Bahnhof","Romanshorn, Florastrasse","Romanshorn, Kreuzlingerstrasse","Wängi, Froberg","Wängi, Weiertal","Weinfelden, Berufsbildungszentrum","Weinfelden, Deucherstrasse","Weinfelden, Nollenstrasse","Weinfelden, Weid")


# Liste der Messstationen beziehen
messstandorte <- get_messstandorte()

# Messstandorte filtern
messtandorte_tg <-  messstandorte %>% 
  filter(nameLang %in% messstationen_tg)


# Nur Messstationen, nicht Passivsammler
stationen_tg <- messtandorte_tg %>% 
  filter(art=="messstation") %>% 
  pull(messstationId)


# Alle verfügbaren Messgrössen beziehen
messgroessen_av <- get_messgroessen(paste0(stationen_tg,collapse = "%2C"))
av_messgroessen <- messgroessen_av$messgroesse$messgroesseId %>% unique()

# Alle verfügbaren Zeitfenster pro Messgrösse
zeitfenster_av <- get_zeitfenster(paste0(messgroessen_av$messgroesse$messgroesseId,collapse = "%2C"))
av_time <- zeitfenster_av$zeitfenster %>% distinct()



# Leere Liste für neue Daten
ostluft_data_full <- list()


# Daten beziehen (immer 10 Tage rückwirkend)
# for (i in 1){
for (i in 1:nrow(av_time)){
  ostluft_data_full[[av_time$name[i]]] <- list()
  # print(av_time$name[i])
  for (mg in av_messgroessen){
    mg_name <- messgroessen_av$messgroesse$name[which(mg == messgroessen_av$messgroesse$messgroesseId)]
    # print(mg_name)
    
    ostluft_data_full[[av_time$name[[i]]]][[mg_name]] <- tryCatch({
      df <- get_ostluft_data(messstationId = stationen_tg,messgroesseId = mg,zeitfensterId = av_time$zeitfensterId[i],days_from_today = 10)
      df[, colSums(!is.na(df)) > 0]
      
    },error = function(cond){
      # print(cond)
      
      NULL
      
    })
  }
}


# Daten zusammenführen und speichern
for (df_name in names(ostluft_data_full)){
  temp_name <- av_time %>% 
    filter(name == df_name) %>% 
    mutate(select_titel = ifelse(titel=="empty zeitfenster",name,titel)) %>% 
    pull(select_titel) %>% 
    clean_string()
  print(temp_name)
                      
  if (file.exists(paste0("data/",temp_name,".rds"))){
    
    df_list <- ostluft_data_full[[df_name]]
    df <-  lapply(seq_along(df_list), function(i){
      prepare_as_tidy_data(df_list[[i]],names(df_list)[i],messstandorte)
      
    }) %>% bind_rows()
    
    current_data <- readRDS(paste0("data/",temp_name,".rds"))
    
    if(length(current_data)>0){
      combined_data <- current_data %>% 
        filter(startzeit < min(df$startzeit)) %>% 
        bind_rows(df) %>% 
        distinct()
      
      saveRDS(combined_data,paste0("data/",temp_name,".rds"))
      
      
      if (temp_name %in% c("gleitender_jahresmittelwert","monatswert")){
        write.table(combined_data, file = paste0("current_data/",temp_name,"_current.csv"), quote = T, sep = ",", dec = ".",
                    row.names = F, na = "",fileEncoding = "utf-8")
      } else {
        save_data_by_year(combined_data, temp_name, output_dir = "csv_gz")
        save_last_365_days(combined_data, temp_name, output_dir = "current_data")
      }
      
      RCurl::ftpUpload(paste0("current_data/",temp_name,"_current.csv"),
                       paste0("ftp://potyhaqi.cyon.site/ostluft/",temp_name,"_current.csv"),
                       userpwd = paste0("OGDTG@potyhaqi.cyon.site:",Sys.getenv("OGDTG_USERPWD"))
                       
      )
      
      # write.table(combined_data, file = ,paste0("csv/",temp_name,".csv"), quote = T, sep = ",", dec = ".", 
      #             row.names = F, na = "",fileEncoding = "utf-8")

    }
  }
}
