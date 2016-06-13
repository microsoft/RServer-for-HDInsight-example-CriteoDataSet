# ----------------------------------------------------------------------------
# set compute context
# ----------------------------------------------------------------------------
# check the system
isLinux <- Sys.info()["sysname"] == "Linux"

useHDFS <- isLinux
useRxSpark <- isLinux

# change this depending on your cluster info
if (Sys.info()["nodename"] == "ed00-lixun2") {
  rxOptions(hdfsHost = "wasb://lixun2sparkcontainer@lixunsparkstorage.blob.core.windows.net")
}

# change the value of dataDir to point to the correct folder
if (useHDFS) {
  # use Hadoop-compatible Distributed File System
  rxOptions(fileSystem = RxHdfsFileSystem())
  
  dataDir <- "/lixun/CriteoAzure"
} else {
  # use Native, Local File System
  rxOptions(fileSystem = RxNativeFileSystem())
  dataDir <- file.path(getwd(), "")
}

if (useRxSpark) {
  # distributed computing using Spark
  computeContext <- RxSpark(executorCores = 1,
                            executorMem = "10g",
                            executorOverheadMem = "10g",
                            consoleOutput = TRUE)  
} else {
  computeContext <- RxLocalSeq()
}

rxSetComputeContext(computeContext)

# ----------------------------------------------------------------------------
# define some utility functions because they work only under the local compute context
# ----------------------------------------------------------------------------
rxRocCurve <- function(...){
  rxSetComputeContext(RxLocalSeq())
  
  rxRocCurve <- RevoScaleR::rxRocCurve(...)
  
  rxSetComputeContext(computeContext)
}

rxRoc <- function(...){
  rxSetComputeContext(RxLocalSeq())
  
  roc <- RevoScaleR::rxRoc(...)
  
  rxSetComputeContext(computeContext)
  
  return(roc)
}
