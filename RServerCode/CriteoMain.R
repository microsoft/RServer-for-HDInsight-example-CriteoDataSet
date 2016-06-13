# criteo analysis with SparkR and MRS

# ----------------------------------------------------------------------------
# get compute context parameters
# ----------------------------------------------------------------------------
source("SetComputeContext.R")

# ----------------------------------------------------------------------------
# general parameters
# ----------------------------------------------------------------------------
# save the current time
time_start <- proc.time()

# to make results replicable
set.seed(1)

# report rows processed and timings
rxOptions(reportProgress = 2) 

# frequency threshold for the categorical variables
t_percent <- 0.05

# variable lists
myvars_I <- c("I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", 
              "I9", "I10", "I11", "I12", "I13")

myvars_C <- c("C1", "C2", "C3", "C4", 
              "C5", "C6", "C7", "C8", "C9", 
              "C10", "C11", "C12", "C13", 
              "C14", "C15", "C16", "C17", 
              "C18", "C19", "C20", "C21", 
              "C22", "C23", "C24", "C25", 
              "C26")

myvars <- c(myvars_I, myvars_C)

# save the current time
time_read_data <- proc.time()

# ----------------------------------------------------------------------------
# extract frequent values for each categorical variable
# ----------------------------------------------------------------------------

colInfo <- list(V1 = list(newName = "group", type="character"),
                V2 = list(newName = "count", type="numeric"),
                V3 = list(newName = "varId", type="integer"))

# read the summary info for whole dataset into memory                  
summaryCatTXT <- RxTextData(file.path(dataDir, "summaryCategoricalCleaned0"), colInfo = colInfo)
summaryCatXDF <- RxXdfData(file.path(dataDir, "summaryCategoricalXdf"))
rxImport(inData = summaryCatTXT,
         outFile = summaryCatXDF, 
         overwrite = TRUE)
         
summary_temp <- rxDataStep(inData = summaryCatXDF)
total_obs <- summary_temp$count

# define the frequency threshold
Cvar_Thr <- total_obs * t_percent

# read the summary info for individual variables into memory   
summaryCatTXT <- RxTextData(file.path(dataDir, "summaryCategoricalCleaned"), colInfo = colInfo)
summaryCatXDF <- RxXdfData(file.path(dataDir, "summaryCategoricalXdf"))

rxImport(inData = summaryCatTXT,
         outFile = summaryCatXDF, 
         rowSelection = (count > CvarThr),
         transformObjects = list(CvarThr = Cvar_Thr), 
         overwrite = TRUE)

summary_temp <- rxDataStep(inData = summaryCatXDF)

summary_temp$group <- sapply(summary_temp$group, function(x) gsub("\"", "", x))

i <- 1 # column id of the first categorical variable
for (var_name in myvars_C) {
  assign(paste0("myset_", var_name), c(as.character(summary_temp[summary_temp$varId == i,1]), 'ffffffff'))
  i <- i + 1
}

# save the current time
time_get_summary_cat <- proc.time()

# ----------------------------------------------------------------------------
# import means for integers
# ----------------------------------------------------------------------------
colInfoInt <- list(V1 = list(newName = "mean", type="numeric"))
summaryIntTXT <- RxTextData(file.path(dataDir, "summaryInteger"), colInfo = colInfoInt)
summaryIntXDF <- RxXdfData(file.path(dataDir, "summaryIntegerXdf"))

rxImport(inData = summaryIntTXT,
         outFile = summaryIntXDF, 
         overwrite = TRUE)

summary_temp_Int <- rxDataStep(inData = summaryIntXDF)

# ----------------------------------------------------------------------------
# assign means for integers
# ----------------------------------------------------------------------------
i <- 1
for (var in myvars_I){
  mean_v <- summary_temp_Int[i,1]
  assign(paste0(var, "_mean"), mean_v)
  i <- i + 1
}

# ----------------------------------------------------------------------------
# assign a value in preparation for the log operation
# ----------------------------------------------------------------------------
add_value <- 100

# save the current time
time_get_summary_int <- proc.time()

# ----------------------------------------------------------------------------
# import data to XDF and feature engineering
# ----------------------------------------------------------------------------
newVarInfo <- list(V1 = list(newName = "clicked"),
                   V2 = list(newName = "I1"),
                   V3 = list(newName = "I2"),
                   V4 = list(newName = "I3"),
                   V5 = list(newName = "I4"),
                   V6 = list(newName = "I5"),
                   V7 = list(newName = "I6"),
                   V8 = list(newName = "I7"),
                   V9 = list(newName = "I8"),
                   V10 = list(newName = "I9"),
                   V11 = list(newName = "I10"),
                   V12 = list(newName = "I11"),
                   V13 = list(newName = "I12"),
                   V14 = list(newName = "I13"),
                   V15 = list(newName = "C1"),
                   V16 = list(newName = "C2"),
                   V17 = list(newName = "C3"),
                   V18 = list(newName = "C4"),
                   V19 = list(newName = "C5"),
                   V20 = list(newName = "C6"),
                   V21 = list(newName = "C7"),
                   V22 = list(newName = "C8"),
                   V23 = list(newName = "C9"),
                   V24 = list(newName = "C10"),
                   V25 = list(newName = "C11"),
                   V26 = list(newName = "C12"),
                   V27 = list(newName = "C13"),
                   V28 = list(newName = "C14"),
                   V29 = list(newName = "C15"),
                   V30 = list(newName = "C16"),
                   V31 = list(newName = "C17"),
                   V32 = list(newName = "C18"),
                   V33 = list(newName = "C19"),
                   V34 = list(newName = "C20"),
                   V35 = list(newName = "C21"),
                   V36 = list(newName = "C22"),
                   V37 = list(newName = "C23"),
                   V38 = list(newName = "C24"),
                   V39 = list(newName = "C25"),
                   V40 = list(newName = "C26"))

mydataCSV <- RxTextData(file.path(dataDir, "CSV"), 
                        colInfo = newVarInfo)
mydataXdf <- RxXdfData(file.path(dataDir, "XDF"))

rxImport(inData = mydataCSV, outFile = mydataXdf, 
         transforms = list(
           # fill in missing values
           I1=(ifelse(is.na(I1), I1_Mean, I1)),
           I2=(ifelse(is.na(I2), I2_Mean, I2)),
           I3=(ifelse(is.na(I3), I3_Mean, I3)),
           I4=(ifelse(is.na(I4), I4_Mean, I4)),
           I5=(ifelse(is.na(I5), I5_Mean, I5)),
           I6=(ifelse(is.na(I6), I6_Mean, I6)),
           I7=(ifelse(is.na(I7), I7_Mean, I7)),
           I8=(ifelse(is.na(I8), I8_Mean, I8)),
           I9=(ifelse(is.na(I9), I9_Mean, I9)),
           I10=(ifelse(is.na(I10), I10_Mean, I10)),
           I11=(ifelse(is.na(I11), I11_Mean, I11)),
           I12=(ifelse(is.na(I12), I12_Mean, I12)),
           I13=(ifelse(is.na(I13), I13_Mean, I13)),
           
           # get floor to reduce unique values
           I1_new = as.integer(floor(log(I1+addvalue)^2)),
           I2_new = as.integer(floor(log(I2+addvalue)^2)),
           I3_new = as.integer(floor(log(I3+addvalue)^2)),
           I4_new = as.integer(floor(log(I4+addvalue)^2)),
           I5_new = as.integer(floor(log(I5+addvalue)^2)),
           I6_new = as.integer(floor(log(I6+addvalue)^2)),
           I7_new = as.integer(floor(log(I7+addvalue)^2)),
           I8_new = as.integer(floor(log(I8+addvalue)^2)),
           I9_new = as.integer(floor(log(I9+addvalue)^2)),
           I10_new = as.integer(floor(log(I10+addvalue)^2)),
           I11_new = as.integer(floor(log(I11+addvalue)^2)),
           I12_new = as.integer(floor(log(I12+addvalue)^2)),
           I13_new = as.integer(floor(log(I13+addvalue)^2)),
           
           # character variables - set missing value to "ffffffff"
           C1_new=(ifelse(is.na(C1), "ffffffff", C1)),
           C2_new=(ifelse(is.na(C2), "ffffffff", C2)),
           C3_new=(ifelse(is.na(C3), "ffffffff", C3)),
           C4_new=(ifelse(is.na(C4), "ffffffff", C4)),
           C5_new=(ifelse(is.na(C5), "ffffffff", C5)),
           C6_new=(ifelse(is.na(C6), "ffffffff", C6)),
           C7_new=(ifelse(is.na(C7), "ffffffff", C7)),
           C8_new=(ifelse(is.na(C8), "ffffffff", C8)),
           C9_new=(ifelse(is.na(C9), "ffffffff", C9)),
           C10_new=(ifelse(is.na(C10), "ffffffff", C10)),
           C11_new=(ifelse(is.na(C11), "ffffffff", C11)),
           C12_new=(ifelse(is.na(C12), "ffffffff", C12)),
           C13_new=(ifelse(is.na(C13), "ffffffff", C13)),
           C14_new=(ifelse(is.na(C14), "ffffffff", C14)),
           C15_new=(ifelse(is.na(C15), "ffffffff", C15)),
           C16_new=(ifelse(is.na(C16), "ffffffff", C16)),
           C17_new=(ifelse(is.na(C17), "ffffffff", C17)),
           C18_new=(ifelse(is.na(C18), "ffffffff", C18)),
           C19_new=(ifelse(is.na(C19), "ffffffff", C19)),
           C20_new=(ifelse(is.na(C20), "ffffffff", C20)),
           C21_new=(ifelse(is.na(C21), "ffffffff", C21)),
           C22_new=(ifelse(is.na(C22), "ffffffff", C22)),
           C23_new=(ifelse(is.na(C23), "ffffffff", C23)),
           C24_new=(ifelse(is.na(C24), "ffffffff", C24)),
           C25_new=(ifelse(is.na(C25), "ffffffff", C25)),
           C26_new=(ifelse(is.na(C26), "ffffffff", C26)),
           
           # replace rare values with "eeeeeeee"
           C1_new_norm = (ifelse(C1_new %in% mySetC1, C1_new, "eeeeeeee")),
           C2_new_norm = (ifelse(C2_new %in% mySetC2, C2_new, "eeeeeeee")),
           C3_new_norm = (ifelse(C3_new %in% mySetC3, C3_new, "eeeeeeee")),
           C4_new_norm = (ifelse(C4_new %in% mySetC4, C4_new, "eeeeeeee")),
           C5_new_norm = (ifelse(C5_new %in% mySetC5, C5_new, "eeeeeeee")),
           C6_new_norm = (ifelse(C6_new %in% mySetC6, C6_new, "eeeeeeee")),
           C7_new_norm = (ifelse(C7_new %in% mySetC7, C7_new, "eeeeeeee")),
           C8_new_norm = (ifelse(C8_new %in% mySetC8, C8_new, "eeeeeeee")),
           C9_new_norm = (ifelse(C9_new %in% mySetC9, C9_new, "eeeeeeee")),
           C10_new_norm=(ifelse(C10_new %in% mySetC10, C10_new, "eeeeeeee")),
           C11_new_norm=(ifelse(C11_new %in% mySetC11, C11_new, "eeeeeeee")),
           C12_new_norm=(ifelse(C12_new %in% mySetC12, C12_new, "eeeeeeee")),
           C13_new_norm=(ifelse(C13_new %in% mySetC13, C13_new, "eeeeeeee")),
           C14_new_norm=(ifelse(C14_new %in% mySetC14, C14_new, "eeeeeeee")),
           C15_new_norm=(ifelse(C15_new %in% mySetC15, C15_new, "eeeeeeee")),
           C16_new_norm=(ifelse(C16_new %in% mySetC16, C16_new, "eeeeeeee")),
           C17_new_norm=(ifelse(C17_new %in% mySetC17, C17_new, "eeeeeeee")),
           C18_new_norm=(ifelse(C18_new %in% mySetC18, C18_new, "eeeeeeee")),
           C19_new_norm=(ifelse(C19_new %in% mySetC19, C19_new, "eeeeeeee")),
           C20_new_norm=(ifelse(C20_new %in% mySetC20, C20_new, "eeeeeeee")),
           C21_new_norm=(ifelse(C21_new %in% mySetC21, C21_new, "eeeeeeee")),
           C22_new_norm=(ifelse(C22_new %in% mySetC22, C22_new, "eeeeeeee")),
           C23_new_norm=(ifelse(C23_new %in% mySetC23, C23_new, "eeeeeeee")),
           C24_new_norm=(ifelse(C24_new %in% mySetC24, C24_new, "eeeeeeee")),
           C25_new_norm=(ifelse(C25_new %in% mySetC25, C25_new, "eeeeeeee")),
           C26_new_norm=(ifelse(C26_new %in% mySetC26, C26_new, "eeeeeeee")),
           
           # train validate data
           TrainValidate = (ifelse(rbinom(.rxNumRows, 
                                          size = 1, 
                                          prob = 0.8), 
                                   "train", 
                                   "validate"))
         ),
         transformObjects = list(I1_Mean = I1_mean,
                                 I2_Mean = I2_mean,
                                 I3_Mean = I3_mean,
                                 I4_Mean = I4_mean,
                                 I5_Mean = I5_mean,
                                 I6_Mean = I6_mean,
                                 I7_Mean = I7_mean,
                                 I8_Mean = I8_mean,
                                 I9_Mean = I9_mean,
                                 I10_Mean = I10_mean,
                                 I11_Mean = I11_mean,
                                 I12_Mean = I12_mean,
                                 I13_Mean = I13_mean,
                                 
                                 addvalue = add_value,
                                 
                                 mySetC1 = myset_C1,
                                 mySetC2 = myset_C2,
                                 mySetC3 = myset_C3,
                                 mySetC4 = myset_C4,
                                 mySetC5 = myset_C5,
                                 mySetC6 = myset_C6,
                                 mySetC7 = myset_C7,
                                 mySetC8 = myset_C8,
                                 mySetC9 = myset_C9,
                                 mySetC10 = myset_C10,
                                 mySetC11 = myset_C11,
                                 mySetC12 = myset_C12,
                                 mySetC13 = myset_C13,
                                 mySetC14 = myset_C14,
                                 mySetC15 = myset_C15,
                                 mySetC16 = myset_C16,
                                 mySetC17 = myset_C17,
                                 mySetC18 = myset_C18,
                                 mySetC19 = myset_C19,
                                 mySetC20 = myset_C20,
                                 mySetC21 = myset_C21,
                                 mySetC22 = myset_C22,
                                 mySetC23 = myset_C23,
                                 mySetC24 = myset_C24,
                                 mySetC25 = myset_C25,
                                 mySetC26 = myset_C26
         ),
         overwrite = TRUE)

rxGetInfo(mydataXdf, getVarInfo = TRUE, numRows = 5)

# save the current time
time_feature_engineering <- proc.time()

# ----------------------------------------------------------------------------
# create factor variables
# ----------------------------------------------------------------------------
# write to txt so that columns can be imported as factors
tempCsv <- RxTextData(file.path(dataDir, "CSV_temp"))
rxDataStep(inData = mydataXdf, 
           outFile = tempCsv, 
           overwrite = TRUE)

# import select columns
vars_to_keep <- c("clicked", "I1_new", 	"I2_new",	"I3_new",	"I4_new",	"I5_new",	"I6_new",	"I7_new",	
                  "I8_new",	"I9_new",	"I10_new",	"I11_new",	"I12_new",	"I13_new",	
                  "C1_new_norm",	"C2_new_norm",	"C3_new_norm",	"C4_new_norm",	"C5_new_norm",	
                  "C6_new_norm",	"C7_new_norm",	"C8_new_norm",	"C9_new_norm",	"C10_new_norm",	
                  "C11_new_norm",	"C12_new_norm",	"C13_new_norm",	"C14_new_norm",	"C15_new_norm",	
                  "C16_new_norm",	"C17_new_norm",	"C18_new_norm",	"C19_new_norm",	"C20_new_norm",	
                  "C21_new_norm",	"C22_new_norm",	"C23_new_norm",	"C24_new_norm",	"C25_new_norm",	
                  "C26_new_norm", "TrainValidate")
tempCsv <- RxTextData(file.path(dataDir, "CSV_temp"), varsToKeep = vars_to_keep)
trainXdf_factor <- RxXdfData(file.path(dataDir, "XdfFactor"))
colClasses <- c("integer", rep("factor", (length(vars_to_keep)-1)))
names(colClasses) <- vars_to_keep 
rxImport(inData = tempCsv, 
         outFile = trainXdf_factor, 
         colClasses = colClasses,
         varsToKeep = vars_to_keep, 
         overwrite = TRUE)
rxGetInfo(trainXdf_factor, getVarInfo = TRUE, numRows = 5)

# save the current time
time_create_factors_All <- proc.time()

# ----------------------------------------------------------------------------
# separate out the datasets
# ----------------------------------------------------------------------------
# split out the training data
train_cleaned <- RxXdfData(file.path(dataDir, "finalDataTrain" ))
rxDataStep(inData = trainXdf_factor, 
           outFile = train_cleaned,
           rowSelection = (TrainValidate == 'train'), 
           overwrite = TRUE)
rxGetInfo(train_cleaned, getVarInfo = TRUE, numRows = 5)

# split out the validation data
validate_cleaned <- RxXdfData( file.path(dataDir, "finalDataValidate" ))
rxDataStep(inData = trainXdf_factor, 
           outFile = validate_cleaned,
           rowSelection = (TrainValidate =='validate'), 
           overwrite = TRUE)
rxGetInfo(validate_cleaned, getVarInfo = TRUE, numRows = 5)

# get count info
total_rows_train <- rxGetInfo(train_cleaned)$numRows
total_rows_validate <- rxGetInfo(validate_cleaned)$numRows

# save the current time
time_split_data <- proc.time()

# ----------------------------------------------------------------------------
# formula for model
# ----------------------------------------------------------------------------
# for full dataset
myformula_model <- formula(train_cleaned, 
                           depVars = 'clicked', 
                           varsToDrop=c("TrainValidate"))

# for testing data
# pred_vars <- c("I1_new", "C1_new_norm")
# myformula_model <- as.formula(paste("clicked ~ ", 
#                                    paste(pred_vars, collapse= "+")))

# ----------------------------------------------------------------------------
# decision tree
# ----------------------------------------------------------------------------
# train model
Tree <- rxDTree(myformula_model, 
                data = train_cleaned,
                cp = 0.001, 
                reportProgress = 2)

# save the current time
time_train_model_tree <- proc.time()

# make predictions
evaluateData <- train_cleaned 
evaluateData <- validate_cleaned 
mypred <- RxXdfData(file.path(dataDir, "treePredict"))

rxPredict(Tree, evaluateData, mypred, writeModelVars = TRUE,
          overwrite = TRUE, predVarNames = c("predicted_clicked"))
rxGetInfo(mypred, getVarInfo = TRUE, numRows = 5)

# plot ROC
rxRocCurve(actualVarName = "clicked", 
           predVarNames = "predicted_clicked",
           data = mypred)

# save the plot
dev.copy(png,'../roc_plot_tree.png')
dev.off()      

# calculate AUC
roc_df <- rxRoc(actualVarName = "clicked", 
                predVarNames = "predicted_clicked",
                data = mypred)

head(roc_df)
rxAuc(roc_df)

# save the current time
time_prediction_validate_tree <- proc.time()

# save the model
save(Tree, file = "../dTreeModel.RData")

# ----------------------------------------------------------------------------
# calculate time and save
# ----------------------------------------------------------------------------
t_total <- time_prediction_validate_tree - time_start
t_read_data <- (time_read_data - time_start)[3]
t_get_summary_cat <- (time_get_summary_cat - time_read_data)[3]
t_get_summary_int <- (time_get_summary_int - time_get_summary_cat)[3]
t_feature_engineering <- (time_feature_engineering - time_get_summary_int)[3]
t_create_factors_All <- (time_create_factors_All - time_feature_engineering)[3]
t_split_data <- (time_split_data - time_create_factors_All)[3]
t_train_model_tree <- (time_train_model_tree - time_split_data)[3]
t_prediction_validate_tree <- (time_prediction_validate_tree - time_train_model_tree)[3]

Item <- c("total_rows_train", 
          "total_rows_validate",
          "time_user", 
          "time_system", 
          "time_elapsed", 
          "time_read_data",
          "time_get_summary_cat",
          "time_get_summary_int",
          "time_feature_engineering",
          "time_create_factors_All",
          "time_split_data",
          "time_train_model_tree",
          "time_prediction_validate_tree"
)

Number <- c(total_rows_train,
            total_rows_validate,
            t_total[1], 
            t_total[2], 
            t_total[3],
            t_read_data,
            t_get_summary_cat,
            t_get_summary_int, 
            t_feature_engineering,
            t_create_factors_All,
            t_split_data,
            t_train_model_tree,
            t_prediction_validate_tree
)

total_time_df <- as.data.frame(cbind(Item = Item, Number = Number))
write.csv(total_time_df, "time_log_mrs_only_no_rxSummary_rxFactors.csv", row.names = FALSE)
