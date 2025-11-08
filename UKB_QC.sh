#################################################################################################
#####################################Genetic QC in UK Biobank####################################
####################################Author: Scott Chiesa 2025####################################
#################################################################################################

## Environment: UK Biobank RAP 
## Tools: Terminal and Swiss Army Knife
## Input File: UKB_QC.sh
## Output Destination: /project/QCed_Genetic_Data
## Other files needed: ancestry_related_sex_removals.R
## Instance: mem1_hdd1_v2_x2 for terminal and mem1_ssd1_v2_x36 for swiss army knife
## Priority: Normal
## Runtime: Terminal (~10mins) Swiss Army Knife (~1hr)
## Cost: ~£3
## Starting n = 487,411
## Final n = 336,797

## What this script does -
 # Removes variants with a minor allele frequency < 1%
 # Removes multi-allelic variants
 # Removes imputed variants with an INFO score < 0.3
 # Removes non-white ancestry individuals
 # Removes related individuals
 # Removes sex-discordant individuals
 # Removes individuals with chromosomal aneuploidy

## What this script does not do -
 # Test whether variants meet Hardy–Weinberg equilibrium (HWE) expectations (due to sample size)
 # Test for variant or participant missingness (as UKB has QCed previously and this is automatically captured below in section testing for ancestry, relatedness, etc)
 # Test for and remove palindromic variants or attempt to align/flip alleles (as this was originally written to later be used in PRSice which does this for you)

 ## How to run -
  # Use dx download to add this shell script to your working space in the RAP
  # Submit the script in a terminal on the UKB RAP with the command 'bash UKB_QC.sh'

 ## Note: If running any individual bits in terminal to troubleshoot, the three lines below must be run after any of the dx upload commands or newly uploaded files won't be found in later commands 
  #umount /mnt
  #mkdir -p /mnt
  #/home/dnanexus/dxfuse -readOnly /mnt /home/dnanexus/.dxfuse_manifest.json


#===============Identify Non-White, Related, or Sex Discordant Individuals==============#

## First create a data dictionary with all available phenotypes (only needs to be done first time or will fail)

# dx extract_dataset project-J1qvq8QJ69pGx77pKyKFb915:record-J41BK60JjX0Z1QfyzZJFVbGj -ddd --delimiter ","

    ## Then extract the necessary fields for QC
    ## These are ID, reported sex, genetic sex, white ancestry, chromosomal aneuploidy, and whether they were used by UKB to calculate PCs (which means they're not related to anyone else)

rm -f ./keep.csv

    echo "Extracting Dataset..."

dx extract_dataset project-XXXXXXXXXXXXXXXXXX:record-XXXXXXXXXXXXXXXXXXXXXXX \
    --fields participant.eid,participant.p31,participant.p22001,participant.p22006,participant.p22019,participant.p22020 \
    -o ./keep.csv

# Now create a file to merge with genotypes while filtering out undesirables
    ## Final output is csv with FID, IID, sex, genetic sex, white ancestry, no aneuploidy, no relatives, and all other phenotypes

# Run R Script

Rscript "/mnt/project/Scripts/ancestry_related_sex_removals.R"

# Take csv file and keep only IID and FID with no headers to use in plink command

rm -f /mnt/project/Datafiles/qced_keep.txt

    echo "converting output file to have proper headers"

awk -F, 'NR>1 {print $1, $2}' ./participants_to_keep.csv > ./qced_keep.txt

    echo "dx uploading participants_to_keep txt file"

dx upload qced_keep.txt --destination /Datafiles/
rm ./qced_keep.txt

#=======Identify multi-allelic variants and remove==========#

rm -f /mnt/project/Datafiles/chr*_qc1_dup.txt

##Identify sites and print list of these to a text file for each chromosome

for i in {1..22}; do
    mfi="/mnt/project/Bulk/Imputation/UKB imputation from genotype/ukb22828_c${i}_b0_v3.mfi.txt"
    outfile="./chr${i}_qc1_dup.txt"

    echo "Processing multi-allelic sites chr${i}..."

    # Reset the output file
    > "$outfile"

    # Count multiallelic sites and save rsIDs
    awk -v chr=$i 'BEGIN{FS="\t"} {
        c[$2]++
    }
    END {
        n=0
        for (k in c) {
            if (c[k] > 1) {
                print k >> "'"$outfile"'"
                n++
            }
        }
        print "chr" chr ": " n " multiallelic sites"
    }' "$mfi"
done

# upload these back to project space to use later

    echo "dx uploading multi-allelic sites csvs"

dx upload ./chr*_qc1_dup.txt --destination /Datafiles/
rm -f ./chr*_qc1_dup.txt


#=============Identify only variants with INFO score > 0.3 so we can keep==========#

rm -f /mnt/project/Datafiles/chr*_info03.snplist

for i in {1..22}; do

    echo "Processing info scores chr${i}..."

  awk '{print $3}' "/mnt/project/Bulk/Imputation/UKB imputation from genotype/ukb22828_c${i}_b0_v3.mfi.txt" \
  | sort | uniq -d > multi.tmp

  awk 'NR==FNR{multi[$1]; next} !($3 in multi) && $8 >= 0.3 {print $2}' multi.tmp \
    "/mnt/project/Bulk/Imputation/UKB imputation from genotype/ukb22828_c${i}_b0_v3.mfi.txt" \
    > "chr${i}_info03.snplist"

  rm multi.tmp
done

    echo "dx uploading info score snplists"

dx upload chr*_info03.snplist --destination /Datafiles/
rm ./chr*_info03.snplist


#============Now convert to pgen files that 1) filter out MAF < 1%, 2) remove any variants with INFO <0.3 3) include only eligible people identified above, and 4) remove any multi-allelic sites===========#

# Use plink to recreate new pgens using a loop which submits every chromosome to Swiss Army Knife to be processed in parallel 

    echo "submitting QCs to Swiss Army Knife"

for i in {1..22}; do
    qc="plink2 --bgen ukb22828_c${i}_b0_v3.bgen ref-first --sample ukb22828_c${i}_b0_v3.sample --keep qced_keep.txt --exclude chr${i}_qc1_dup.txt --extract chr${i}_info03.snplist --maf 0.01 --make-pgen --out ./chr_${i}_qc1"

    dx run swiss-army-knife \
     -iin="/Bulk/Imputation/UKB imputation from genotype/ukb22828_c${i}_b0_v3.bgen" \
     -iin="/Bulk/Imputation/UKB imputation from genotype/ukb22828_c${i}_b0_v3.sample" \
     -iin="/Datafiles/qced_keep.txt" \
     -iin="/Datafiles/chr${i}_qc1_dup.txt" \
     -iin="/Datafiles/chr${i}_info03.snplist" \
     -icmd="${qc}" \
     --tag="QC_chr${i}" \
     --instance-type "mem1_ssd1_v2_x36" \
     --destination="project-XXXXXXXXXXXXXXXXXX:/QCed_Genetic_Data" --brief --yes
done