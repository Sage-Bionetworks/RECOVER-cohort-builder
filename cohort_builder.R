#############################
# Cohort builder: Parquet folder structure on PILOT data (syn50996868)
# Folder structure: datasetType->participant->file
#############################

########
# Required Libraries
########
library(arrow)
library(synapser)
library(tidyverse)
library(synapserutils)

########
# Set up Access and download dataset
########
synapser::synLogin()

###########
# Get pilot data
###########
synapserutils::syncFromSynapse(entity = 'syn50996868',
                               path = './pilot_data')

## syncFromSynapse adds a manifest file. need to remove them all
pilot_files_to_remove <- list.files('./pilot_data/', recursive = T) %>% 
  as.data.frame() %>% 
  `colnames<-`(c('parquet_pilot_path')) %>% 
  dplyr::filter(grepl('SYNAPSE_METADATA', parquet_pilot_path))

for(path_to_remove in pilot_files_to_remove$parquet_pilot_path){
  file_to_remove <- paste0('./pilot_data/', path_to_remove)
  print(file_to_remove)
  unlink(file_to_remove)
}

rm(pilot_files_to_remove)

valid_paths_pilot_df <- list.dirs('./pilot_data') %>% 
  as.data.frame() %>% 
  `colnames<-`(c('parquet_path_pilot')) %>% 
  dplyr::rowwise() %>% 
  dplyr::filter(grepl('dataset',parquet_path_pilot)) %>% 
  dplyr::mutate(datasetType = str_split(parquet_path_pilot,'/')[[1]][3]) %>%
  dplyr::ungroup() %>%
  dplyr::filter(!datasetType == 'dataset_healthkitv2electrocardiogram')

# dataset_healthkitv2electrocardiogram - these files are JSON in pilot data, we will look at dataset_healthkitv2electrocardiogram separately

#####################
# We need the data at datasetType->participant (parquet folder levels)
# We have data at datasetType
#####################
# Read parquet for each datasetType into one file 
# Currently we do all of this in one go for all datasets. For prod data this needs to be done
# individually for each dataset because of size
all_parquet <- lapply(valid_paths_pilot_df$datasetType, function(datasetType){
  temp_parquet <- arrow::open_dataset(paste0('./pilot_data/',datasetType)) %>% dplyr::collect()
})

# Need to partition at participant Identifier level, does dataset have that column?
parquet_with_participantId <- lapply(all_parquet, function(parquet.df){
  if('ParticipantIdentifier' %in% colnames(parquet.df)){
    return(TRUE)
  }else{
    return(FALSE)
  }
})

aa <- lapply(all_parquet, function(parquet.df){
  if('StartDate' %in% colnames(parquet.df)){
    return(TRUE)
  }else{
    return(FALSE)
  }
})


##### Build parquet datasets needed for cohorts
## First lets consider only those datasets that have ParticipantIdentifier column 
# Some datasets have logId to map between logId and ParticipantIdentifier from a sister dataset
# for eg., "dataset_fitbitsleeplogs_sleeplogdetails" does not have ParticipantIdentifier, but just logId, 
# which in conjunction with "dataset_fitbitsleeplogs" can give ParticipantIdentifier
#####
metadata_files_all <- lapply(seq(length(parquet_with_participantId)), function(i){
  # datasets that have ParticipantIdentifier column 
  if(parquet_with_participantId[[i]]){
    print(valid_paths_pilot_df$datasetType[i])
    
    test_data <- all_parquet[[i]]
    datasetType <- valid_paths_pilot_df$datasetType[i]
    has_participant_id <- parquet_with_participantId[[i]]
    
    test_data %>% 
      dplyr::group_by(ParticipantIdentifier) %>% 
      arrow::write_dataset(paste0('cohort_builder/', datasetType), format = 'parquet', hive_style = FALSE)
    
    meta_data_files <- list.files(paste0('cohort_builder/', datasetType), recursive = T) %>% 
      as.data.frame() %>% 
      `colnames<-`('filePath') %>% 
      dplyr::rowwise() %>% 
      dplyr::mutate(ParticipantIdentifier = substr(filePath, 1,13)) %>% 
      dplyr::mutate(filePath = paste0('cohort_builder/', datasetType,'/',filePath)) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(datasetType = datasetType)
    
    return(meta_data_files)
  }
})

### Check the 'cohort builder' folder for the curated data