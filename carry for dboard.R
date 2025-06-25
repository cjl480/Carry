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

############# Functions - may look to add some of these to FIRVR ################

# Holdings data function is from functions.R in active mkt monitor

get_holdings_data <- function(CURRENCIES = c("USD", "CAD", "AUD", "EUR", "JPY")){
  
  OL_trade_data <-  openlink_repo_query()
  
  #table breaking out the holdings for each portfolio
  allhold <-
    position_tplus(OL_trade_data, t_plus_day = t_plus_date(as.numeric(1))) %>%
    dplyr::filter(CURRENCY %in% CURRENCIES, abs(Position) >  0.1) %>%
    dplyr::mutate(
      PORTFOLIO_TYPE = case_when(
        INTERNAL_PORTFOLIO %in% c(
          "ABM - AUD$ Bond Mgt",
          "CBM - CAN$ Bond Mgt",
          "UBM - US$ Bond Mgt",
          "EBM - European Bond Mgt",
          "YBM - YEN Bond Mgt"
        ) ~ "Management",
        INTERNAL_PORTFOLIO %in% c(
          "ABA - AUD$ Bond Trading",
          "CBA - CAN$ Bond Trading",
          "UBA - US$ Bond Trading",
          "EBA - European Bond Trading",
          "YBA - Yen Bond Trading"
        ) ~ "Active",
        INTERNAL_PORTFOLIO %in% c(
          "ANRL",
          "ENRL - \u20AC Net Reserves Liability",
          "ENRL",
          "ENRL - \u20AC Net Reserves Liab",
          "UNRL",
          "CNRL",
          "YNRL"
        ) ~ "Unhedged",
        TRUE ~ "Hedged"
      )
    ) %>%
    dplyr::select(-c(INTERNAL_PORTFOLIO)) %>%
    tidyr::spread(PORTFOLIO_TYPE, Position) 
  
  #Putting zeros in for NAs in portfolios with no holdings and add empty col if no holdings in that portfolio 
  if(!("Unhedged" %in% colnames(allhold))){allhold <- allhold %>% mutate(Unhedged = 0)}
  if(!("Hedged" %in% colnames(allhold))){allhold <- allhold %>% mutate(Hedged = 0)}
  if(!("Management" %in% colnames(allhold))){allhold <- allhold %>% mutate(Management = 0)}
  if(!("Active" %in% colnames(allhold))){allhold <- allhold %>% mutate(Active = 0)}
  allhold <- replace_na(allhold, replace = list(Active = 0, Management = 0, Hedged = 0, Unhedged = 0))
  
  
  # filter to get a table of repo collateral only 
  dfr_repo <- allhold %>%
    dplyr::filter(TRAN_TYPE == "Repo Coll") %>%
    dplyr::mutate(Repo = Active + Management + Hedged + Unhedged) %>%
    dplyr::select(c(TICKER, INS_NUM, ISIN, Repo))
  
  #filter table for holdings then join repo collateral table and format 
  holdings_report <- allhold %>%
    dplyr::filter(TRAN_TYPE == "Trading") %>%
    dplyr::mutate(Done = Active + Management + Hedged + Unhedged) %>%
    dplyr::full_join(dfr_repo, by = c("TICKER", "INS_NUM", "ISIN")) %>%
    tidyr::replace_na(replace = list(Active = 0, Management = 0, Hedged = 0, Unhedged = 0, Repo = 0, Done = 0)) %>%
    dplyr::mutate(Total = Done + Repo) %>%
    dplyr::select(-c(TRAN_TYPE, CURRENCY)) %>% 
    dplyr::mutate_if(is.numeric, round, digits = 3) %>%
    dplyr::mutate(instruments =  gsub(pattern = "_", " ", TICKER), 
                  issuer_coupon = gsub('.{6}$', '', instruments), 
                  date = stringr:: str_sub(instruments, - 6, - 1), 
                  day = stringr:: str_sub(date, 1, 2),
                  month = stringr:: str_sub(date, 3, 4), 
                  year = stringr:: str_sub(date, 5, 6), 
                  date = paste0(month, "/", day, "/", year)) %>% 
    dplyr::select(-day, -month, -year, -TICKER) %>% 
    dplyr::mutate(security = paste0(issuer_coupon, date), 
                  security =  gsub(pattern = "MM ", "", security))
  
  return(holdings_report)
}


# position_tplus function is from functions.R in active mkt monitor

position_tplus <- function(position_data, t_plus_day = Sys.Date()){
  
  # Sort relevant trades
  posit_dfr <- position_data %>% 
    filter(INS_TYPE %in% c("GBND", "MM-G-Bill", "MM-ECP", "BONDFUT", "IBOND", "DFUT")) %>% #not including repo, cash depos or swaps
    filter(SETTLE_DATE <= t_plus_day, MATURITY_DATE > t_plus_day) %>%   # filter for unmatured instruments that have settled on day t_plus_day 
    select(c(TICKER, INS_NUM, ISIN, INTERNAL_PORTFOLIO, TRAN_TYPE, CURRENCY, POSITION)) %>%      #select columns wanted from the 30
    group_by(TICKER, INS_NUM, ISIN, CURRENCY, INTERNAL_PORTFOLIO, TRAN_TYPE) %>%         
    summarise(Position = sum(POSITION), .groups = "keep") %>%              # sum across all positions to get net position by portfolio 
    ungroup()
  
  posit_dfr 
}

# t_plus_date function is from functions.R in active mkt monitor

t_plus_date <- function(t_plus_days = 1L, ref_day = Sys.Date()){
  
  if(t_plus_days < 0) flog.error("t_plus_days(): t_plus less than zero")
  if(t_plus_days != round(t_plus_days, digits = 0)) flog.error("t_plus_days(): t_plus is not an integer")
  if(!(is.Date(ref_day))) flog.error("t_plus_days(): ref_date is not a date")
  if(wday(ref_day) == 7 | wday(ref_day) == 1) flog.warn("t_plus_days(): ref_date is a weekend")
  
  t_plus_days_mod <- t_plus_days %% 5 
  add_days <- ((t_plus_days - t_plus_days_mod) / 5) * 7
  
  wday_t_plus <- wday(ref_day + t_plus_days_mod) %% 7
  if (wday_t_plus < t_plus_days_mod) add_days <- add_days + 2
  
  t_plus_date <- ref_day + add_days + t_plus_days_mod
  
  if(wday(t_plus_date) == 7 | wday(t_plus_date) == 1)  flog.error("t_plus_days(): output a weekend")
  
  t_plus_date
}

# Estimating repo rates (new code in functions.R)

get_repo_rates <- function(rfr_data, cur) { # Requires currency and the repo adjustment vs OIS (in basis points) as inputs
  
  static_data <- repo_config %>%
    filter(currency == cur)
  
  ois_curves <- left_join(static_data, rfrs, by = 'ticker') %>% # Add back required fields
    mutate(funding_rate = PX_LAST + as.numeric(adjust/100)) %>% # Need to reduce by factor of 100 as given in bp. 
    select(currency, tenor, adjust, funding_rate) 
  
  return(ois_curves)
  
}


# Carry function (new code in functions.R)


find_carry <- function(data, bond_list, months) {
  
  
  df <- data %>%
    filter(ListName == bond_list)
  
  cur <- unique(df %>%
                  pull(CRNCY))
  
  repo_rates <- get_repo_rates(rfrs, cur) %>%
    filter(tenor == months)
  
  adjust <- repo_rates %>%
    pull(adjust) %>%
    as.numeric()
  
  x <- left_join(df, repo_rates, by = c("CRNCY" = "currency")) %>% # Join Bloomberg returns data and repo rates to calculate carry figures.
    mutate("carry_yld" = (YLD_YTM_MID - funding_rate) / DURATION * months/12 * 100, # Multiply up to get carry in bps
           "carry_asw" = (YAS_ASW_SPREAD - adjust) / DURATION * months/12)   # Already in bps 
}
####### DATA & CONFIG ######################################################

# Load RFR, holdings, carryroll data (bbg logic to save down new RFR & carry/roll files is in config.R script). Note pointing to local copies on C drive.

rfrs <- readr::read_csv(file = "C:/RM_Projects/4.3.3/ActiveDashboard/Tasks/active_monitor/data/repo_data.csv", show_col_types = FALSE)

repo_config <- readr::read_csv("C:/RM_Projects/4.3.3/ActiveDashboard/Tasks/active_monitor/data/repo_config.csv")

holdings <- get_holdings_data()

carry_roll_df <- readr::read_csv(file = "C:/RM_Projects/4.3.3/ActiveDashboard/Tasks/active_monitor/data/all_securities.csv",
                                 col_types = cols(ASSET_SWAP_SPD_MID = col_double(), 
                                                  ASW_SPREAD_RBACOR = col_double(), 
                                                  ASW_SPREAD_BBSW6m = col_double())) %>%
  dplyr::left_join(holdings, by = c("ID_ISIN" = "ISIN")) %>%
  mutate(AMOUNT_HELD_BY_EEA = format(round(Done/(AMT_OUTSTANDING/1e08), 2), nsmall = 2)) %>%
  mutate(YAS_ASW_SPREAD = case_when(
    CRNCY == "EUR" ~ ASW_SPREAD_ESTR,
    TRUE ~ YAS_ASW_SPREAD
  )) 


########### TESTING - use ABA ##################################################

# Load all swaps in the ABA portfolio from OL:

swaps <- FIRVrBOE::openlink_query_api(portfolios = "ABA - AUD$ Bond Trading",
                                   instrument_types = "IRS",
                                   tran_statuses = "Validated")


# Add classification for these swaps for ease of analysis

swap_classify <- swaps %>%
  mutate(direction = case_when(
       
             POSITION < 0 ~ "PAY",
             TRUE ~ "REC")) %>%
  select(DEAL_TRACKING_NUM, INS_TYPE, POSITION, MATURITY_DATE, direction)


# Pull 3m RBACOR. When improving, should link this to swap reference rate dynamically. 

ad_3m <- rfrs %>%
  filter(ticker == "ADSOC BGN Curncy") %>%
  pull(PX_LAST)

# Calculate carry for existing swaps in OL. 

swap_carry <- swaps %>%
  filter(SETTLE_DATE <= today()) %>% # Fwd start swaps have no carry
  mutate(spread_bps = 10000 * PRICE - 100 * ad_3m, # Spread between fixed rate and anticipated floating rate over next three months
         carry_cash = (spread_bps/10000 * POSITION) * (90/360)) %>% #Should do for 3m approx
  select(DEAL_TRACKING_NUM, carry_cash) 


# Load all bond trades from OL

bonds <- FIRVrBOE::openlink_query_api(portfolios = "ABA - AUD$ Bond Trading",
                                        instrument_types = "GBND",
                                        tran_statuses = "Validated")

# Add classification for these bonds for ease of analysis

bond_classify = bonds %>%
  mutate(direction = case_when(
    POSITION < 0 ~ "SHORT",
    TRUE ~ "LONG")) %>%
  select(DEAL_TRACKING_NUM, TICKER, INS_TYPE, POSITION, MATURITY_DATE, direction)

# Calculate carry for various AUD lists in listmanager. Then bind together

govs <- find_carry(carry_roll_df, "AUD_Govt_AU", months = 3)
states <- find_carry(carry_roll_df, "AUD_States", months = 3)
ssa <- find_carry(carry_roll_df, "AUD_Agencies", months = 3)

aussie <- rbind(govs, states, ssa) %>%
  select(ID_ISIN, carry_yld, DURATION)


# Join carry calculation per bond to the holdings data:

bond_carry <- left_join(bonds, aussie, by = c( "ISIN" = "ID_ISIN")) %>%
  mutate(
    carry_bps = carry_yld,
    dv01 = PROCEEDS * DURATION / -10000, # # Using proceeds * duration is a reasonable approx for DV01 - but not perfect as proceeds is from initial trade. 
    carry_cash = carry_yld * dv01 ) %>%
  select(DEAL_TRACKING_NUM, carry_cash)

# Join swap and bond carry together. 

all_carry <- rbind(bond_carry, carry)


################# Find carry per strategy ####################################


# Find deal number mapping  for ABA

config_data <- read_csv("N:/Offdata/RM/_Dashboard/P&L Config/config/strategy_trade_mapping.csv")

aba_data <- config_data %>%
  filter(Portfolio == "ABA")


# Initiate an empty dataframe

result_df <- data.frame( 
  StrategyID = as.character(),
  `3m_carry` = character())

unique_strats <- unique(aba_data$StrategyID)


# For every unique strategy, find the sum of carry across the relevant deal numbers.

for (i in 1:length(unique_strats)) { 
  
  current_strat <- unique_strats[i]
  
# Find relevant deal numbers
  
 deals <- aba_data %>%
    filter(StrategyID == current_strat) %>%
    pull(`Deal No`)
 
 # Filter for only these deals

filtered_deals <- all_carry %>%
  filter(DEAL_TRACKING_NUM %in% deals)

# Sum carry across deals and add to the empty results data frame

carry <- sum(filtered_deals$carry_cash, na.rm = TRUE)

result_df <- rbind(result_df, data.frame(
  StrategyID = current_strat,
  `3m_carry` = carry ))

}

# Add back in some of the extra descriptive data:

distinct_strats <- aba_data %>%
  distinct(StrategyID, .keep_all = TRUE)

test <- left_join(distinct_strats, result_df, by = "StrategyID") 



#### For a given strategy, show carry across individual components ###########


# Individual components for each strategy

strategy <- 1437

  h <- aba_data %>%
    filter(StrategyID == strategy) %>%
    pull(`Deal No`)
  
  z <- all_carry %>%
    filter(DEAL_TRACKING_NUM %in% h)
  
  # Add back in details 
  
  # Initial join
  by_deal <- left_join(z, aba_data, by = c("DEAL_TRACKING_NUM" = "Deal No"))
  
  # Conditionally join bond_classify
  if (nrow(semi_join(bond_classify, by_deal, by = "DEAL_TRACKING_NUM")) > 0) {
    by_deal <- left_join(by_deal, bond_classify, by = "DEAL_TRACKING_NUM")
  }
  
  # Conditionally join swap_classify
  if (nrow(semi_join(swap_classify, by_deal, by = "DEAL_TRACKING_NUM")) > 0) {
    by_deal <- left_join(by_deal, swap_classify, by = "DEAL_TRACKING_NUM")
  }


   by_deal_group <- by_deal %>%
    group_by(TICKER, Portfolio, Level0, StrategyID, Strategy, MATURITY_DATE) %>%
    summarise (carry_cash = sum(carry_cash),
               POSITION = sum(POSITION))

