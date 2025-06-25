
.libPaths()


.libPaths(c(
  "C:/RM_Projects//4.3.3/ActiveDashboard/Packages//4.3.3/2025-03-31",
  "C:/Program Files/R/R-4.3.3/library"
))
# Packages

library(FIRVr)
library(InflationTools)
library(DBI)
library(dplyr)
library(flock)
library(futile.logger)
library(httr)
library(lubridate)
library(purrr)
library(Rblpapi)
library(readr)
library(RQuantLib)
library(stringr)
library(tibble)
library(tidyr)
library(XLConnect)
library(zoo)
library(FIRVrBOE)



swaps <- FIRVr::openlink_query_api(portfolios = "ABA",
                     instrument_types = "IRS",
                     tran_statuses = "Validated")
