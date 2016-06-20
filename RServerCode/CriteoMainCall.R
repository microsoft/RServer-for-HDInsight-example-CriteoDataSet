# ----------------------------------------------------------------------------
# this is the script for calling the main code
# ----------------------------------------------------------------------------
# set the working directory
# setwd("F:/CriteoSummaries") # local
setwd("/home/lixzhang/lixun/Test") # on Spark

wd <- getwd()
console_output <- "console_output.log"
setwd(wd)
con <- file(console_output)
sink(con, append=TRUE) # this line is required for initiating the file
sink(con, append=TRUE, type="message")

# echo all input and not truncate 150+ character lines...
source("CriteoMain.R", echo=TRUE, max.deparse.length=10000)

# restore output to console
sink() # this line is required to avoid duplicating output
sink(type="message")

# look at the log
setwd(wd) # return to the correct directory for test.log
cat(readLines(console_output), sep="\n")
