library(dplyr)
library(readxl)

#-------------------------------------------------------------------------------------------------
# CONFIGURATION
#-------------------------------------------------------------------------------------------------
RSCRIPTS_HOME = sprintf("%s/5_rstudio/fintools", Sys.getenv("PROJECTS"))
CONFIG_HOME = sprintf("%s/2_config/fintools", Sys.getenv("PROJECTS"))
# WORK_DIR = sprintf("%s/fintools", Sys.getenv("LOGS_DIR"))
WORK_DIR = "D:/10 PROJECTS/zworkarea/IA"
JAR_ROOT = sprintf("%s/1_dist/IA/dist-20200220-1800", Sys.getenv("PROJECTS"))

source(sprintf("%s/utils.R", RSCRIPTS_HOME))

TRACELOG_INIT(sprintf("%s\\99_IA_execution_log.txt", WORK_DIR))

#-------------------------------------------------------------------------------------------------
# METHOD TO RUN JAR FILE
#-------------------------------------------------------------------------------------------------
execute_IA <- function(filename, txn_filter, inv_type) {
  TRACELOG(sprintf("execute_IA: %s, %s, %s", filename, txn_filter, inv_type))
  IA_input = read_excel(filename, "ORDERS")
  if (inv_type == "MF") {
    IA_input = subset(IA_input, grepl(txn_filter, TXNID))
    IA_input = IA_input[ , -which(names(IA_input) %in% c("ERR", "REM"))]
    IA_input$NAV = sprintf(IA_input$NAV, fmt="%#.4f")
    IA_input$Unit = sprintf(IA_input$Unit, fmt="%#.4f")
    IA_input$AMOUNT = sprintf(IA_input$AMOUNT, fmt="%#.4f")
    IA_input$TXNDATE = format(strptime(IA_input$TXNDATE, format="%Y-%m-%d"), "%d-%b-%y")
  } else if (inv_type == "EQ") {
    IA_input = subset(IA_input, grepl(txn_filter, TxnId))
    IA_input = select(IA_input, TxnId, Date, Stock, Action, Qty, Rate, Value, BROK)
    IA_input$Rate = sprintf(IA_input$Rate, fmt="%#.4f")
    IA_input$Value = sprintf(IA_input$Value, fmt="%#.4f")
    IA_input$BROK = sprintf(IA_input$BROK, fmt="%#.4f")
    IA_input$Date = format(strptime(IA_input$Date, format="%Y-%m-%d"), "%d-%b-%y")
  }
  
  TRACELOG(sprintf("%s: %d valid rows, %d cols", filename, nrow(IA_input), ncol(IA_input)))
  write.table(IA_input, sprintf("%s/03-IA-input.txt", WORK_DIR), row.names=FALSE, col.names=TRUE, sep="\t", quote=FALSE)
  
  command = sprintf("java -jar \"%s\\IA.jar\" 1 %s DIRECT", JAR_ROOT, inv_type)
  TRACELOG(sprintf("Executing JAR: [%s]", command))
  outcome = system(command, intern=TRUE)
  
  TRACELOG("outcome of JAR command::::")
  TRACELOG("--------------------------------------------------------------------------------------")
  TRACELOG(outcome)
  TRACELOG("--------------------------------------------------------------------------------------")
  if (grepl("ERROR", paste(outcome, collapse = ""))) {
    TRACELOG("ERROR!! -------------------------------------------------------------------")
    TRACELOG("ERROR!! ------------------ EXECUTION SEEMS TO HAVE FAILED -----------------")
    TRACELOG("ERROR!! -------------------------------------------------------------------")
    return(FALSE)
  }

  return(TRUE)
}

#-------------------------------------------------------------------------------------------------
# INITIALIZE
#-------------------------------------------------------------------------------------------------
TOTAL_CG   = NULL
TOTAL_PAMT = NULL
TOTAL_SAMT = NULL
CG_STP     = NULL
CG_STNP    = NULL
CG_LTP     = NULL
CG_LTNP    = NULL
LTCG_LIST  = NULL

#-------------------------------------------------------------------------------------------------
# ONE CLICK
#-------------------------------------------------------------------------------------------------
consolidate <- function(filename, txn_filter, inv_type) {
  if (execute_IA(filename, txn_filter, inv_type)) {
    TRACELOG(sprintf("execute_IA: SUCCESSFULLY PROCESSED: [%s] [%s]", inv_type, filename))
    CG    = read.csv(sprintf("%s/93_SUMMARY_CG.csv", WORK_DIR))
    PAMT  = read.csv(sprintf("%s/94_SUMMARY_PAMT.csv", WORK_DIR))
    SAMT  = read.csv(sprintf("%s/95_SUMMARY_SAMT.csv", WORK_DIR))
    CGPP  = read.csv(sprintf("%s/96_CG_PER_PERIOD.csv", WORK_DIR))
    LTCG  = read.csv(sprintf("%s/91_TXN_LTCG.csv", WORK_DIR))
    
    if (is.null(TOTAL_CG)) {
      TOTAL_CG <<- CG
      TOTAL_PAMT <<- PAMT
      TOTAL_SAMT <<- SAMT
      CG_STP  <<- subset(CGPP, CGTYPE=="STP")
      CG_STNP <<- subset(CGPP, CGTYPE=="STNP")
      CG_LTP  <<- subset(CGPP, CGTYPE=="LTP")
      CG_LTNP <<- subset(CGPP, CGTYPE=="LTNP")
      LTCG_LIST <<- LTCG
    } else {
      cgtypes = list("STP", "STNP", "LTP", "LTNP")
      TOTAL_CG <<- bind_rows(lapply(cgtypes, FUN=function(cgtype) {
        return(data.frame(
          CGTYPE=cgtype, PERIOD="CG",
          subset(TOTAL_CG, CGTYPE==cgtype)[ , -which(names(TOTAL_CG) %in% c("CGTYPE", "PERIOD"))] +
          subset(CG, CGTYPE==cgtype)[ , -which(names(CG) %in% c("CGTYPE", "PERIOD"))]
        ))
      }))
      
      TOTAL_PAMT <<- bind_rows(lapply(cgtypes, FUN=function(cgtype) {
        return(data.frame(
          CGTYPE=cgtype, PERIOD="PAMT",
          subset(TOTAL_PAMT, CGTYPE==cgtype)[ , -which(names(TOTAL_PAMT) %in% c("CGTYPE", "PERIOD"))] +
          subset(PAMT, CGTYPE==cgtype)[ , -which(names(PAMT) %in% c("CGTYPE", "PERIOD"))]
        ))
      }))
      
      TOTAL_SAMT <<- bind_rows(lapply(cgtypes, FUN=function(cgtype) {
        return(data.frame(
          CGTYPE=cgtype, PERIOD="SAMT",
          subset(TOTAL_SAMT, CGTYPE==cgtype)[ , -which(names(TOTAL_SAMT) %in% c("CGTYPE", "PERIOD"))] +
          subset(SAMT, CGTYPE==cgtype)[ , -which(names(SAMT) %in% c("CGTYPE", "PERIOD"))]
        ))
      }))
      
      ##################### ATP #####################
      atptypes = list("ATP1", "ATP2", "ATP3", "ATP4")
      CG_STP <<- bind_rows(lapply(atptypes, FUN=function(atptype) {
        return(data.frame(
          CGTYPE="STP", PERIOD=atptype,
          subset(CG_STP, CGTYPE=="STP" & PERIOD==atptype)[ , -which(names(CG_STP) %in% c("CGTYPE", "PERIOD"))] +
          subset(CGPP, CGTYPE=="STP" & PERIOD==atptype)[ , -which(names(CGPP) %in% c("CGTYPE", "PERIOD"))]
        ))
      }))
      
      CG_STNP <<- bind_rows(lapply(atptypes, FUN=function(atptype) {
        return(data.frame(
          CGTYPE="STNP", PERIOD=atptype,
          subset(CG_STNP, CGTYPE=="STNP" & PERIOD==atptype)[ , -which(names(CG_STNP) %in% c("CGTYPE", "PERIOD"))] +
          subset(CGPP, CGTYPE=="STNP" & PERIOD==atptype)[ , -which(names(CGPP) %in% c("CGTYPE", "PERIOD"))]
        ))
      }))
      
      CG_LTP <<- bind_rows(lapply(atptypes, FUN=function(atptype) {
        return(data.frame(
          CGTYPE="LTP", PERIOD=atptype,
          subset(CG_LTP, CGTYPE=="LTP" & PERIOD==atptype)[ , -which(names(CG_LTP) %in% c("CGTYPE", "PERIOD"))] +
          subset(CGPP, CGTYPE=="LTP" & PERIOD==atptype)[ , -which(names(CGPP) %in% c("CGTYPE", "PERIOD"))]
        ))
      }))
      
      CG_LTNP <<- bind_rows(lapply(atptypes, FUN=function(atptype) {
        return(data.frame(
          CGTYPE="LTNP", PERIOD=atptype,
          subset(CG_LTNP, CGTYPE=="LTNP" & PERIOD==atptype)[ , -which(names(CG_LTNP) %in% c("CGTYPE", "PERIOD"))] +
          subset(CGPP, CGTYPE=="LTNP" & PERIOD==atptype)[ , -which(names(CGPP) %in% c("CGTYPE", "PERIOD"))]
        ))
      }))
      ##################### LTCG #####################
      if (nrow(LTCG) != 0) {
        LTCG_LIST <<- bind_rows(LTCG_LIST, LTCG)
      } else {
        TRACELOG(sprintf("%s: Empty LTCG, ignoring", filename))
      }
    }
  }
  else {
    TRACELOG(sprintf("execute_IA: ERROR IN PROCESSING: [%s] [%s]", inv_type, filename))
  }
}

#-------------------------------------------------------------------------------------------------
# START
#-------------------------------------------------------------------------------------------------
args = commandArgs(trailingOnly=TRUE)
if (length(args) == 2) {
  option = args[1]
  input_file = args[2]
} else {
  option = "0"
  input_file = "input_1.xlsx"
}

TRACELOG(sprintf("Args: option: %s, input_file: %s\n", option, input_file))
TRACELOG(sprintf("RSCRIPTS_HOME: %s", RSCRIPTS_HOME))
TRACELOG(sprintf("CONFIG_HOME:   %s", CONFIG_HOME))
TRACELOG(sprintf("WORK_DIR:      %s", WORK_DIR))
TRACELOG(sprintf("JAR_ROOT:      %s", JAR_ROOT))

if (TRUE) {
  inputs = read_excel(sprintf("%s/%s", CONFIG_HOME, input_file))
}

if (TRUE) {
  TRACELOG("--------------------------------------------------------------------------------------")
  TRACELOG("Removing old files ...")
  files.list = list("90_HOLDINGS.csv", "91_TXN_LIST.csv", "91_TXN_LTCG.csv", "92_CG_DETAILS.csv",
                    "93_SUMMARY_CG.csv", "94_SUMMARY_PAMT.csv", "95_SUMMARY_SAMT.csv", "96_CG_PER_PERIOD.csv",
                    "97-OVERALL_CG.csv", "98-OVERALL_LTCG.csv",
                    "03-IA-input.txt", "99-IA-trace.txt")
  res = lapply(files.list, FUN=function(filename) {
    full_path = sprintf("%s\\%s", WORK_DIR, filename)
    TRACELOG(sprintf("%s --> %s", full_path, delete_file(full_path)))
  })
  TRACELOG("All files removed.")
  TRACELOG("--------------------------------------------------------------------------------------")
}

if (TRUE) {
  if (option == "0") {
    res = lapply(1:nrow(inputs), FUN=function(rowno) {
      file_name = as.character(inputs$'file name'[rowno])
      txn_prefix = as.character(inputs$'txn prefix'[rowno])
      data_type = as.character(inputs$'data type'[rowno])
      TRACELOG("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      TRACELOG(sprintf('JAR options: file_name: [%s], txn_prefix: [%s], data_type: [%s]',
                       file_name, txn_prefix, data_type))
      consolidate(file_name, txn_prefix, data_type)
      TRACELOG("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    })
  } else {
    row = subset(inputs, ID == option)
    file_name = as.character(row['file name'])
    txn_prefix = as.character(row['txn prefix'])
    data_type = as.character(row['data type'])
    TRACELOG(sprintf('JAR options: file_name: [%s], txn_prefix: [%s], data_type: [%s]',
                     file_name, txn_prefix, data_type))
    consolidate(file_name, txn_prefix, data_type)
  }
  
  total_cg_file =   sprintf("%s/97-OVERALL_CG.csv", WORK_DIR)
  total_ltcg_file = sprintf("%s/98-OVERALL_LTCG.csv", WORK_DIR)
  
  write.table(TOTAL_CG,   total_cg_file, row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE)
  write.table(TOTAL_PAMT, total_cg_file, row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE, append=TRUE)
  write.table(TOTAL_SAMT, total_cg_file, row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE, append=TRUE)
  write.table(CG_STP,     total_cg_file, row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE, append=TRUE)
  write.table(CG_STNP,    total_cg_file, row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE, append=TRUE)
  write.table(CG_LTP,     total_cg_file, row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE, append=TRUE)
  write.table(CG_LTNP,    total_cg_file, row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE, append=TRUE)
  write.table(LTCG_LIST,  total_ltcg_file, row.names=FALSE, col.names=TRUE, sep=",", quote=FALSE)
  
  TRACELOG("############# R SCRIPT EXECUTION COMPLETED #############")
}

dump_warnings()

# dump_warnings(warnings_list)
#-------------------------------------------------------------------------------------------------


