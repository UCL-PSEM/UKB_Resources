## This file will be automatically run by UKB_QC.sh provided it is in the right directory

# Load library needed

library(dplyr)

# Read in the csv generated above

df <- read.csv("./keep.csv", stringsAsFactors = FALSE)

# Create flags for failures
    ## This checks if reported sex matches genetic, if ancestry is recorded as White-British (1), if aneuploidy is reported, and if they were included in generation of PCs

df <- df %>%
  mutate(
    sex_mismatch = participant.p31 != participant.p22001,
    non_white_british = participant.p22006 != 1 | is.na(participant.p22006),
    aneuploidy = !is.na(participant.p22019),
    related = participant.p22020 != 1 | is.na(participant.p22020)
  )

# Count how many fail each criterion and print list
    ## Lets us see why people were removed

qc_summary <- df %>%
  summarise(
    total = n(),
    fail_sex_mismatch = sum(sex_mismatch, na.rm = TRUE),
    fail_non_white_british = sum(non_white_british, na.rm = TRUE),
    fail_aneuploidy = sum(aneuploidy, na.rm = TRUE),
    fail_related = sum(related, na.rm = TRUE),
    pass_all = sum(!sex_mismatch & !non_white_british & !aneuploidy & !related, na.rm = TRUE)
  )

print(qc_summary)

# Now apply QC filters for necessary output file

df_qced <- df %>%
  filter(
    participant.p31 == participant.p22001, # reported sex == genetic sex
    participant.p22006 == 1,               # White British ancestry
    is.na(participant.p22019),             # No sex chromosome aneuploidy
    participant.p22020 == 1                # Used in PCA (unrelated)
  )

# Rename columns

df_qced <- df_qced %>%
  rename(
    IID = participant.eid,
    sex = participant.p31,
    genetic_sex = participant.p22001,
    white_ancestry = participant.p22006,
    no_aneuploidy = participant.p22019,
    no_relatives = participant.p22020
  )

# Add FID column (same as IID)
df_qced$FID <- df_qced$IID

# Select columns for phenotype table
cols <- c("FID", "IID", "sex", "genetic_sex", "white_ancestry", "no_aneuploidy", "no_relatives")
df_phenotype <- df_qced[, cols]

# Save output
write.csv(df_phenotype, "./participants_to_keep.csv", row.names = FALSE)