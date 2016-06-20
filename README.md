# RServer-for-HDInsight-example-CriteoDataSet
This repo contains a walkthrough of how to use RServer for HDInsight with large data sets like Criteo.

## Running Instructions
It took about 10 hours to run the analysis on my cluster using the Criteo data for day 14 - day 23 (420 GB). You can test your cluster and the program by using a subset of the data, e.g., data for day 14 (46 GB).

### Deploy an HDInsight cluster
More information about how to deploy [R Server for HDInsight](https://azure.microsoft.com/en-us/services/hdinsight/r-server/) can be found at the [documentation site](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-hadoop-r-server-overview/). It is recommended that you install RStudio on the cluster by following the [instructions](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-hadoop-r-server-install-r-studio/) as well. Here's the information on the cluster I deployed:

| Type         | Cores | RAM (GB) | Nodes | Pricing Tier |
|--------------|------:|---------:|------:|--------------|
| Head Nodes   |    32 |      224 |     2 | D14          |
| Worker Nodes |   960 |    6,720 |    60 | D14          |

### Get the Criteo data 
Information on the data can be found at [Now Available on Azure ML – Criteo's 1TB Click Prediction Dataset](https://blogs.technet.microsoft.com/machinelearning/2015/04/01/now-available-on-azure-ml-criteos-1tb-click-prediction-dataset/). After downloading and extracting data for day 14 - day 23, upload them to a folder on your HDInsight cluster using tools like [AzCopy](https://azure.microsoft.com/en-us/documentation/articles/storage-use-azcopy/).

### Get the summary data
The summary data can be downloaded from [an Azure blob](https://mypublicstorage.blob.core.windows.net/mycontainer/CriteoSummaries.zip). The summary is for the 1 TB data and includes frequency counts for categorical variables and means for integer variables. After downloading and extracting data, upload them to your HDInsight cluster using tools like [AzCopy](https://azure.microsoft.com/en-us/documentation/articles/storage-use-azcopy/).

### Update the programs
SetComputeContext.R
* Enter the nodename of your cluster and update the WASB address.
* Replace the value of *dataDir* with the correct path to where the data is saved. For example, I saved all data for my project in the folder "/lixun/CriteoAzure" so I assiged this path to *dataDir*.

CriteoMain.R
* Update the paths to the raw Criteo data as well as summaries of categorical and integer variables.

CriteoMainCall.R
*  Change the working directory to point to your folder where the programs are saved.

### Run CriteoMainCall.R
For example, you can run the program from RStudio installed on the HDInsight cluster.

---
Created by a Microsoft Employee.  
Copyright (C) Microsoft. All Rights Reserved.
