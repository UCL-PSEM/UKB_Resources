# Genetic QC in UK Biobank  

**Environment:** UK Biobank RAP  
**Tools:** Terminal and Swiss Army Knife  
**Input File:** UKB_QC.sh  
**Output Destination:** /project/QCed_Genetic_Data  
**Other files needed:** ancestry_related_sex_removals.R  
**Instance:** mem1_hdd1_v2_x2 for terminal and mem1_ssd1_v2_x36 for swiss army knife  
**Priority:** Normal  
**Runtime:** Terminal (~10mins) Swiss Army Knife (~1hr)  
**Cost:** ~£3  
**Starting n:** 487,411  
**Final n** 336,797  

## What this script does  

 Removes variants with a minor allele frequency < 1%  
 Removes multi-allelic variants  
 Removes imputed variants with an INFO score < 0.3  
 Removes non-white ancestry individuals  
 Removes related individuals  
 Removes sex-discordant individuals  
 Removes individuals with chromosomal aneuploidy  

 ## What this script doesn't do  

 Test whether variants meet Hardy–Weinberg equilibrium (HWE) expectations (due to sample size)  
 Test for variant or participant missingness (as UKB has QCed previously and this is automatically captured below in section testing for ancestry, relatedness, etc)  
 Test for and remove palindromic variants or attempt to align/flip alleles (as this was originally written to later be used in PRSice which does this for you)  

  ## How to run

  Use dx download to add this shell script to your working space in the RAP  
  Submit the script in a terminal on the UKB RAP with the command 'bash UKB_QC.sh'  

  **Note: If running any individual bits in terminal to troubleshoot, the three lines below must be run after any of the dx upload commands or newly uploaded files won't be found in later commands** 
  
  umount /mnt  
  mkdir -p /mnt  
  /home/dnanexus/dxfuse -readOnly /mnt /home/dnanexus/.dxfuse_manifest.json  
 
