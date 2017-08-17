
library(dplyr)
library(stringr)
library(data.table)

# Create time object in DDMMMYYYY format using US Central time zone to automate this code
current_day <- paste(as.character(format(as.Date(format(as.POSIXct(Sys.time(), tz="Etc/UCT"), tz="America/Chicago",usetz=FALSE)),"%d")),
                     toupper(substring(months(as.Date(format(as.POSIXct(Sys.time(), tz="Etc/UCT"), tz="America/Chicago",usetz=FALSE))),1,3)),
                     as.character(format(as.Date(format(as.POSIXct(Sys.time(), tz="Etc/UCT"), tz="America/Chicago",usetz=FALSE)),"%Y")), sep="")

# Manually set current_day value in DDMMMYYYY format if not runing today's extracts!!
# current_day <- as.character("20JUL2017")

current_day

################################################################################
############################# Step 1 Import data ###############################
################################################################################

Import_file <- function(mydata) {
  
  fpath <- paste0(str_sub(current_day, -4, -1), 
                  as.character(format(as.Date(current_day, "%d%b%Y"),"%m")), 
                  str_sub(current_day, 1, 2))
  
  setwd(paste0("/home/zhaoch/R_Workspace/Acct Growth/Data/V",fpath))
  
  
  data_name <- paste0(deparse(substitute(mydata)), fpath, ".csv")
  
  mydata <- read.csv(data_name, header = T, stringsAsFactors = F, fileEncoding="latin1")
  
  mydata
}

# Call function to import weekly extracts
abp                 <- Import_file(abp)
acct                <- Import_file(acct)
acctcustomercontact <- Import_file(acctcustomercontact)
bugsoppty           <- Import_file(bugsoppty)
bup                 <- Import_file(bup)
bus                 <- Import_file(bus)
compls              <- Import_file(compls)
custprior           <- Import_file(custprior)
opp                 <- Import_file(opp)
partnerlandscape    <- Import_file(partnerls)
scorecard           <- Import_file(scorecard)
stratinit           <- Import_file(stratinit)
tce                 <- Import_file(tce)

################################################################################
######################### Step 2 Change Columns Names ##########################
################################################################################

# Standardize column names for extracts by removing extra periods (space in Excel), otherwise it'll cause problems when appending to Master files
Standard_col <- function(mydata) {
  
  colnames(mydata) <- gsub('(\\.)\\1+', '\\1', colnames(mydata))
  
  colnames(mydata)
}

colnames(abp)                 <- Standard_col(abp)
colnames(acct)                <- Standard_col(acct)
colnames(acctcustomercontact) <- Standard_col(acctcustomercontact)
colnames(bugsoppty)           <- Standard_col(bugsoppty)
colnames(bup)                 <- Standard_col(bup)
colnames(bus)                 <- Standard_col(bus)
colnames(compls)              <- Standard_col(compls)
colnames(custprior)           <- Standard_col(custprior)
colnames(opp)                 <- Standard_col(opp)
colnames(partnerlandscape)    <- Standard_col(partnerlandscape)
colnames(scorecard)           <- Standard_col(scorecard)
colnames(stratinit)           <- Standard_col(stratinit)
colnames(tce)                 <- Standard_col(tce)

# Change colunm names to match Master Data
abp <- abp %>%
  head(nrow(abp) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_ID = Account.Account.ID,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Account.Plan.Name,
         ABP_ACCT_SMMRY_SCORE = Account.Summary.Score,
         ABP_ANNUAL_SLS_CURRENCY = Annual.Sales.M.Currency,
         ABP_ANNUAL_SLS_AMT = Annual.Sales.M.,
         ABP_ANNUAL_SLS_CONV = Annual.Sales.M.converted.Currency,
         ABP_ANNUAL_SLS_AMT_CONV = Annual.Sales.M.converted.,
         ABP_APPROVER = Approver,
         ABP_AVG_SCORE = Average.Score,
         ABP_BUP_SCORE = Business.Unit.Plans.Score,
         ABP_COMPLS_SCORE = Competitive.Landscapes.Score,
         ABP_BUSPRIOR_SCORE = Customer.Business.Priorities.Score,
         ABP_CUST_DFNTN_AGRD = Customer.definition.agreed,
         ABP_CUST_INDSTRY = Industry.Vertical,
         ABP_CUST_PPTITG_DESCR = Customer.PPT.Integration.Description,
         ABP_CUST_RLTNSHP_MAP_SCORE = Customer.Relationship.Maps.Score,
         ABP_CUST_STRAT = Customer.Strategic.Overview,
         ABP_EXEC_ASKS = Executive.Asks,
         ABP_EXEC_SPONSOR = Executive.Sponsor,
         ABP_FISCAL_YEAREND = Fiscal.Year.End,
         ABP_IA_GUIDANCE = Further.Innovation.Agenda.Guidance,
         ABP_Y11_REV_CURRENCY = FY11.Revenue.Total.Currency,
         ABP_Y11_REV_AMT = FY11.Revenue.Total.,
         ABP_Y11_REV_CONV = FY11.Revenue.Total.converted.Currency,
         ABP_Y11_REV_AMT_CONV = FY11.Revenue.Total.converted.,
         ABP_Y12_REV_CURRENCY = FY12.Revenue.Total.Currency,
         ABP_Y12_REV_AMT = FY12.Revenue.Total.,
         ABP_Y12_REV_CONV = FY12.Revenue.Total.converted.Currency,
         ABP_Y12_REV_AMT_CONV = FY12.Revenue.Total.converted.,
         ABP_Y13_REV_CURRENCY = FY13.Revenue.Total.Currency,
         ABP_Y13_REV_AMT = FY13.Revenue.Total.,
         ABP_Y13_REV_CONV = FY13.Revenue.Total.converted.Currency,
         ABP_Y13_REV_AMT_CONV = FY13.Revenue.Total.converted.,
         ABP_Y14_REV_CURRENCY = FY14.Revenue.Total.Currency,
         ABP_Y14_REV_AMT = FY14.Revenue.Total.,
         ABP_Y14_REV_CONV = FY14.Revenue.Total.converted.Currency,
         ABP_Y14_REV_AMT_CONV = FY14.Revenue.Total.converted.,
         ABP_Y15_REV_CURRENCY = FY15.Revenue.Total.Currency,
         ABP_Y15_REV_AMT = FY15.Revenue.Total.,
         ABP_Y15_REV_CONV = FY15.Revenue.Total.converted.Currency,
         ABP_Y15_REV_AMT_CONV = FY15.Revenue.Total.converted.,
         ABP_GOVN = Governance,
         ABP_CUST_DFN_INNVTN = How.our.customer.defines.Innovation,
         ABP_HP_CUST_GOVN = HPE.and.Customer.Governance,
         ABP_GOTOMARKET_SEG = Account.Plan.Classification,
         ABP_HP_INNVTN_STRAT = HPE.Innovation.Strategy.for.Customer,
         ABP_HP_SI_SCORE = HPE.Strategic.Initiatives.Score,
         ABP_STRAT_OPP = HPE.Strategy.Opportunities,
         ABP_HQ_CNTRY = HQ.Country,
         ABP_HQ_REG = HQ.Region,
         ABP_HQ_SUBREG = HQ.Sub.Region,
         ABP_INACTIVE_PLAN = Inactive.Plan,
         ABP_IND_SEG = Industry.Segment,
         ABP_INNVTN_ENGGMNT_REF = Innovation.Engagement.Referenceable,
         ABP_KEY_ISSUE = Key.Challenges,
         ABP_LAST_CUST_ENGGMNT = Last.Customer.Innovation.Engagement,
         ABP_LEGAL_NM = Legal.Name,
         ABP_NEXT_CUST_ENGGMNT = Next.Customer.Innovation.Engagement,
         ABP_NOTE = Note.,
         ABP_NUM_EMPLOYEE = Number.of.Employees,
         ABP_OWNER_EMAIL = Owner.change.email,
         ABP_PARTNERLS_SCORE = Partner.Landscape.Score,
         ABP_PLAN_STATUS = Plan.Status,
         ABP_PRIV_ACCT = Private.Account,
         ABP_HP_CUST_STATE = State.of.HPE.Business.with.Customer,
         ABP_PPTITG_DESCR = Summary.PPT.Integration.Description,
         ABP_TCE_PRIOR = TCE.Priorities,
         ABP_TCE_SCORE = Total.Customer.Experience.Score,
         ABP_TTL_SCORE = Total.Score,
         ABP_TTL_SCORE1 = Total.Score.1,
         ABP_TTL_SCORE_PCT = Total.Score.,
         ABP_URL1 = URL1,
         ABP_URL2 = URL2,
         ABP_URL3 = URL3,
         ABP_CURRENCY = Account.Plan.Currency,
         ABP_OWNER_NM = Account.Plan.Owner.Name,
         ABP_OWNER_ALIAS = Account.Plan.Owner.Alias,
         ABP_OWNER_ROLE = Account.Plan.Owner.Role,
         ABP_CREATOR = Account.Plan.Created.By,
         ABP_CREATED_ALIAS = Account.Plan.Created.Alias,
         ABP_CREATED_DT = Account.Plan.Created.Date,
         ABP_LAST_MOD_NM = Account.Plan.Last.Modified.By,
         ABP_LAST_MOD_ALIAS = Account.Plan.Last.Modified.Alias,
         ABP_LAST_MOD_DT = Account.Plan.Last.Modified.Date,
         ABP_LAST_ACTIVITY_DT = Account.Plan.Last.Activity.Date) %>% 
  
  mutate(ABP_LAST_CUST_ENGGMNT = if_else(nchar(ABP_LAST_CUST_ENGGMNT)==0,as.character(""),
                                         paste(as.character(format(as.Date(ABP_LAST_CUST_ENGGMNT, format = '%m/%d/%Y'),"%d")),
                                               toupper(substring(months(as.Date(ABP_LAST_CUST_ENGGMNT, format = '%m/%d/%Y')),1,3)),
                                               as.character(format(as.Date(ABP_LAST_CUST_ENGGMNT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ABP_NEXT_CUST_ENGGMNT = if_else(nchar(ABP_NEXT_CUST_ENGGMNT)==0,as.character(""),
                                         paste(as.character(format(as.Date(ABP_NEXT_CUST_ENGGMNT, format = '%m/%d/%Y'),"%d")),
                                               toupper(substring(months(as.Date(ABP_NEXT_CUST_ENGGMNT, format = '%m/%d/%Y')),1,3)),
                                               as.character(format(as.Date(ABP_NEXT_CUST_ENGGMNT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ABP_CREATED_DT = if_else(nchar(ABP_CREATED_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(ABP_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(ABP_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(ABP_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ABP_LAST_MOD_DT = if_else(nchar(ABP_LAST_MOD_DT)==0,as.character(""),
                                   paste(as.character(format(as.Date(ABP_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                         toupper(substring(months(as.Date(ABP_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                         as.character(format(as.Date(ABP_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ABP_LAST_ACTIVITY_DT = if_else(nchar(ABP_LAST_ACTIVITY_DT)==0,as.character(""),
                                        paste(as.character(format(as.Date(ABP_LAST_ACTIVITY_DT, format = '%m/%d/%Y'),"%d")),
                                              toupper(substring(months(as.Date(ABP_LAST_ACTIVITY_DT, format = '%m/%d/%Y')),1,3)),
                                              as.character(format(as.Date(ABP_LAST_ACTIVITY_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ABP_CUST_DFNTN_AGRD = as.character(ABP_CUST_DFNTN_AGRD),
         
         ABP_INACTIVE_PLAN = as.character(ABP_INACTIVE_PLAN),
         
         ABP_NUM_EMPLOYEE = as.character(ABP_NUM_EMPLOYEE),
         
         ABP_OWNER_EMAIL = as.character(ABP_OWNER_EMAIL),
         
         ABP_PRIV_ACCT = as.character(ABP_PRIV_ACCT)
  )

acct <- acct %>%
  head(nrow(acct) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_ID = Account.Account.ID,
         ACCT_NM = Account.Account.Name,
         ACCT_OWNER = Account.Account.Owner,
         ACCT_OWNER_ALIAS = Account.Account.Owner.Alias,
         ACCT_CURRENCY = Account.Account.Currency,
         ACCT_ANNUAL_REV_CURRENCY = Account.Annual.Revenue.USD.Currency,
         ACCT_ANNUAL_REV_AMT = Account.Annual.Revenue.USD.,
         ACCT_ANNUAL_REV_CONV = Account.Annual.Revenue.USD.converted.Currency,
         ACCT_ANNUAL_REV_AMT_CONV = Account.Annual.Revenue.USD.converted.,
         ACCT_REC_TYPE = Account.Account.Record.Type,
         ACCT_EMPLOYEE = Account.Employees,
         ACCT_CREATED_DT = Account.Created.Date,
         ACCT_CREATOR = Account.Created.By,
         ACCT_CREATED_ALIAS = Account.Created.Alias,
         ACCT_LAST_MOD_DT = Account.Last.Modified.Date,
         ACCT_LAST_MOD_NM = Account.Last.Modified.By,
         ACCT_LAST_MOD_ALIAS = Account.Last.Modified.Alias,
         ACCT_LAST_ACTIVITY = Account.Last.Activity,
         ACCT_AMID = Account.AMID,
         ACCT_LATIN_NM = Account.Account.Name.Latin.Capture,
         ACCT_ALLIANCE_FLAG = Account.Alliance.Partner.Flag,
         ACCT_ALLIANCE_ROLE = Account.Alliance.Role,
         ACCT_CHNNL_FLAG = Account.Channel.Partner.Flag,
         ACCT_PRIV = Account.Private.Account,
         ACCT_DUN = Account.Site.DUNS,
         ACCT_UDUN = Account.Domestic.DUNS,
         ACCT_GDUN = Account.Global.DUNS,
         ACCT_LOC_ID = Account.Location.ID,
         ACCT_MDCP = Account.MDCP.Organization.ID,
         ACCT_PARENT_PARTNER_ID = Account.Parent.Partner.ID,
         ACCT_PARENT_PARTNER_NM = Account.Parent.Partner.Name,
         ACCT_PARTNER_STATUS = Account.Partner.Status,
         #ACCT_PARTNER_TYPE = Account.Partner.Type, # Edit on 4/13, column removed from SFDC
         ACCT_VALID_REGION = Account.Valid.Region,
         ACCT_SOR_ACCT_ID = Account.Source.System.Account.ID,
         ACCT_ALT_NM = Account.Alternate.Name,
         ACCT_SEG = Account.Customer.Segment,
         #ACCT_IND_SEG = Account.Industry.Segment, # Edit on 4/13, column removed from SFDC
         ACCT_IND_VERT = Account.Industry.Vertical,
         ACCT_LASTGEOASSIGN_DT = Account.LastGeoAssignmentDate,
         ACCT_NAMEDACCT = Account.Named.Account,
         ACCT_STANDARD_ACCT = Account.Standard.Account,
         ACCT_PARTNERPORT_EGLIG = Account.Partner.Portal.Eligible,
         ACCT_RAD = Account.RAD,
         ACCT_TAXID = Account.Tax.Identifier,
         ACCT_NAME2 = Account.Trade.Style.Name.2,
         ACCT_NAME3 = Account.Trade.Style.Name.3,
         ACCT_NAME4 = Account.Trade.Style.Name.4,
         ACCT_NAME5 = Account.Trade.Style.Name.5,
         ACCT_GEO_HIERARCHY = Account.Deleted.Geographic.Hierarchy,
         ACCT_PROFILE = Account.AccountProfileEvaluated,
         ACCT_CHATTER_BL_TYPE = Account.Chatter.Blacklist.Type,
         ACCT_COUNTRY_CD = Account.CountryCode,
         ACCT_HP_LEAD_STAT = Account.HP.Lead.Status,
         ACCT_HP_SFDC_ACCESS = Account.HP.SFDC.Access,
         ACCT_LAST_PROF_DT = Account.LastAcountProfAssignmentDate,
         ACCT_LAST_COVRG_DT = Account.LastCoverageAssignmentDate,
         ACCT_LAST_IND_DT = Account.LastIndustryAssignmentDate,
         ACCT_MDCP_SUB = Account.MDCP.Subscribed,
         ACCT_GEO_HIERARCHY2 = Account.Geographic.Hierarchy,
         ACCT_REG = Account.World.Region,
         # ACCT_SUBREG1 = Account.SubRegion1,  # Edit on 4/20, column removed from SFDC
         # ACCT_SUBREG2 = Account.SubRegion2,  # Edit on 4/20, column removed from SFDC
         # ACCT_SUBREG3 = Account.SubRegion3,  # Edit on 4/20, column removed from SFDC
         ACCT_WORLD_REG = Account.World.Region.1,
         ACCT_BG_TARGET_SEG = Account.BG.Target.Segments,
         ACCT_CASE_SAFE_ID = Account.Case.Safe.ID,
         ACCT_BUS_RELTNSHP = Account.Business.Relationship,
         ACCT_MDCP_ORG_ID = Account.MDCP.Partner.Organization.ID,
         ACCT_MDCP_SITE_ID = Account.MDCP.Site.Instance.ID,
         ACCT_MDCP_SUB_FLAG = Account.MDCP.Subscribed.Flag,
         ACCT_DB_OUTOFBUS_FLAG = Account.D.B.Out.of.Business,
         ACCT_FTP_INFO = Account.FTP.Username.Password,
         ACCT_MDCP_BUS_ID = Account.MDCP.Business.Relationship.ID,
         ACCT_MDCP_LAST_SYNC = Account.MDCP.Last.Synced,
         ACCT_ORG_DUN = Account.Organization.DUNS.Number,
         ACCT_ORG_UNIT = Account.Organization.Unit,
         ACCT_PRIV_BUS_RLTNSHP = Account.Primary.Business.Relationship,
         ACCT_RPL_LAST_SYNC = Account.RPL.Last.Synced,
         ACCT_RPL_STAT = Account.RPL.status,
         ACCT_REC_SUBTYPE = Account.Account.Record.Subtype,
         ACCT_SUPPORT_TYPE = Account.Support.Type,
         ACCT_USERKEY = Account.Userkey) %>%
  
  mutate(# Change DateTime to Date
    ACCT_LASTGEOASSIGN_DT = substr(ACCT_LASTGEOASSIGN_DT,1,
                                   regexpr(" ",ACCT_LASTGEOASSIGN_DT)-1),
    ACCT_LAST_PROF_DT     = substr(ACCT_LAST_PROF_DT,1,
                                   regexpr(" ",ACCT_LAST_PROF_DT)-1),
    ACCT_LAST_COVRG_DT    = substr(ACCT_LAST_COVRG_DT,1,
                                   regexpr(" ",ACCT_LAST_COVRG_DT)-1),
    ACCT_LAST_IND_DT      = substr(ACCT_LAST_IND_DT,1,
                                   regexpr(" ",ACCT_LAST_IND_DT)-1),
    ACCT_MDCP_LAST_SYNC   = substr(ACCT_MDCP_LAST_SYNC,1,
                                   regexpr(" ",ACCT_MDCP_LAST_SYNC)-1),
    ACCT_RPL_LAST_SYNC    = substr(ACCT_RPL_LAST_SYNC,1,
                                   regexpr(" ",ACCT_RPL_LAST_SYNC)-1)) %>%
  
  mutate(ACCT_CREATED_DT = if_else(nchar(ACCT_CREATED_DT)==0,as.character(""),
                                   paste(as.character(format(as.Date(ACCT_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                         toupper(substring(months(as.Date(ACCT_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                         as.character(format(as.Date(ACCT_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ACCT_LAST_MOD_DT = if_else(nchar(ACCT_LAST_MOD_DT)==0,as.character(""),
                                    paste(as.character(format(as.Date(ACCT_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                          toupper(substring(months(as.Date(ACCT_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                          as.character(format(as.Date(ACCT_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ACCT_LAST_ACTIVITY = if_else(nchar(ACCT_LAST_ACTIVITY)==0,as.character(""),
                                      paste(as.character(format(as.Date(ACCT_LAST_ACTIVITY, format = '%m/%d/%Y'),"%d")),
                                            toupper(substring(months(as.Date(ACCT_LAST_ACTIVITY, format = '%m/%d/%Y')),1,3)),
                                            as.character(format(as.Date(ACCT_LAST_ACTIVITY, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ACCT_LASTGEOASSIGN_DT = if_else(nchar(ACCT_LASTGEOASSIGN_DT)==0,as.character(""),
                                         paste(as.character(format(as.Date(ACCT_LASTGEOASSIGN_DT, format = '%m/%d/%Y'),"%d")),
                                               toupper(substring(months(as.Date(ACCT_LASTGEOASSIGN_DT, format = '%m/%d/%Y')),1,3)),
                                               as.character(format(as.Date(ACCT_LASTGEOASSIGN_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ACCT_LAST_PROF_DT = if_else(nchar(ACCT_LAST_PROF_DT)==0,as.character(""),
                                     paste(as.character(format(as.Date(ACCT_LAST_PROF_DT, format = '%m/%d/%Y'),"%d")),
                                           toupper(substring(months(as.Date(ACCT_LAST_PROF_DT, format = '%m/%d/%Y')),1,3)),
                                           as.character(format(as.Date(ACCT_LAST_PROF_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ACCT_LAST_COVRG_DT = if_else(nchar(ACCT_LAST_COVRG_DT)==0,as.character(""),
                                      paste(as.character(format(as.Date(ACCT_LAST_COVRG_DT, format = '%m/%d/%Y'),"%d")),
                                            toupper(substring(months(as.Date(ACCT_LAST_COVRG_DT, format = '%m/%d/%Y')),1,3)),
                                            as.character(format(as.Date(ACCT_LAST_COVRG_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ACCT_LAST_IND_DT = if_else(nchar(ACCT_LAST_IND_DT)==0,as.character(""),
                                    paste(as.character(format(as.Date(ACCT_LAST_IND_DT, format = '%m/%d/%Y'),"%d")),
                                          toupper(substring(months(as.Date(ACCT_LAST_IND_DT, format = '%m/%d/%Y')),1,3)),
                                          as.character(format(as.Date(ACCT_LAST_IND_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         
         ACCT_MDCP_LAST_SYNC = if_else(nchar(ACCT_MDCP_LAST_SYNC)==0,as.character(""),
                                       paste(as.character(format(as.Date(ACCT_MDCP_LAST_SYNC, format = '%m/%d/%Y'),"%d")),
                                             toupper(substring(months(as.Date(ACCT_MDCP_LAST_SYNC, format = '%m/%d/%Y')),1,3)),
                                             as.character(format(as.Date(ACCT_MDCP_LAST_SYNC, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ACCT_RPL_LAST_SYNC = if_else(nchar(ACCT_RPL_LAST_SYNC)==0,as.character(""),
                                      paste(as.character(format(as.Date(ACCT_RPL_LAST_SYNC, format = '%m/%d/%Y'),"%d")),
                                            toupper(substring(months(as.Date(ACCT_RPL_LAST_SYNC, format = '%m/%d/%Y')),1,3)),
                                            as.character(format(as.Date(ACCT_RPL_LAST_SYNC, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         ACCT_ALLIANCE_FLAG = as.character(ACCT_ALLIANCE_FLAG),
         
         ACCT_CHNNL_FLAG = as.character(ACCT_CHNNL_FLAG),
         
         ACCT_PRIV = as.character(ACCT_PRIV),
         
         ACCT_DUN = as.character(ACCT_DUN),
         
         ACCT_UDUN = as.character(ACCT_UDUN),
         
         ACCT_GDUN = as.character(ACCT_GDUN),
         
         ACCT_MDCP = as.character(ACCT_MDCP),
         
         ACCT_STANDARD_ACCT = as.character(ACCT_STANDARD_ACCT),
         
         ACCT_PARTNERPORT_EGLIG = as.character(ACCT_PARTNERPORT_EGLIG),
         
         ACCT_PROFILE = as.character(ACCT_PROFILE),
         
         ACCT_MDCP_SITE_ID = as.character(ACCT_MDCP_SITE_ID),
         
         ACCT_MDCP_BUS_ID = as.character(ACCT_MDCP_BUS_ID),
         
         ACCT_ORG_DUN = as.character(ACCT_ORG_DUN),
         
         ACCT_ORG_UNIT = as.character(ACCT_ORG_UNIT),
         
         ACCT_PRIV_BUS_RLTNSHP = as.character(ACCT_PRIV_BUS_RLTNSHP),
         
         ACCT_PARTNER_STATUS = as.character(ACCT_PARTNER_STATUS),
         
         ACCT_MDCP_SUB = as.character(ACCT_MDCP_SUB),
         
         ACCT_MDCP_SUB_FLAG = as.character(ACCT_MDCP_SUB_FLAG),
         
         ACCT_DB_OUTOFBUS_FLAG = as.character(ACCT_DB_OUTOFBUS_FLAG)
  )

acctcustomercontact <- acctcustomercontact %>%
  head(nrow(acctcustomercontact) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Account.Plan.Name,
         CRM_ID = Customer.Relationship.Map.ID,
         CRM_NM = Customer.Relationship.Map.Customer.Relationship.Map.Name,
         CRM_ADDTNL_INFO = Additional.Information,
         CRM_HPCONTACT = Aligned.HPE.Contact,
         CRM_BGCONTACT = BG.Scope.of.the.contact.,
         CRM_BUCONTACT = BU.Scope.of.the.contact.,
         CRM_POLITICALPOWER_SCORE = Political.Power.Score,
         CRM_PP_S1 = PP_S1,
         CRM_PP_S10 = PP_S10,
         CRM_PP_S11 = PP_S11,
         CRM_PP_S12 = PP_S12,
         CRM_PP_S2 = PP_S2,
         CRM_PP_S3 = PP_S3,
         CRM_PP_S4 = PP_S4,
         CRM_PP_S5 = PP_S5,
         CRM_PP_S6 = PP_S6,
         CRM_PP_S7 = PP_S7,
         CRM_PP_S8 = PP_S8,
         CRM_PP_S9 = PP_S9,
         CRM_RELATIONSHIP_METERSCORE = Relationship.Meter.Score,
         CRM_RELATIONSHIPTYPE = Relationship.Type,
         CRM_REPORTSTO = Reports.To,
         CRM_RM_S1 = RM_S1,
         CRM_RM_S10 = RM_S10,
         CRM_RM_S2 = RM_S2,
         CRM_RM_S3 = RM_S3,
         CRM_RM_S4 = RM_S4,
         CRM_RM_S5 = RM_S5,
         CRM_RM_S6 = RM_S6,
         CRM_RM_S7 = RM_S7,
         CRM_RM_S8 = RM_S8,
         CRM_RM_S9 = RM_S9,
         CRM_ROLE = Role,
         CRM_TITLE = Title,
         CRM_CURRENCY = Customer.Relationship.Map.Currency,
         CRM_CREATOR = Customer.Relationship.Map.Created.By,
         CRM_CREATED_ALIAS = Customer.Relationship.Map.Created.Alias,
         CRM_CREATED_DT = Customer.Relationship.Map.Created.Date,
         CRM_LAST_MOD_NM = Customer.Relationship.Map.Last.Modified.By,
         CRM_LAST_MOD_ALIAS = Customer.Relationship.Map.Last.Modified.Alias,
         CRM_LAST_MOD_DT = Customer.Relationship.Map.Last.Modified.Date,
         CRM_LAST_ACT_DT = Customer.Relationship.Map.Last.Activity.Date,
         CONTACT_FIRSTNAME = Contact.First.Name,
         CONTACT_LASTNAME = Contact.Last.Name,
         CONTACT_CONTACTID = Contact.Contact.ID) %>%
  
  mutate(CRM_CREATED_DT = if_else(nchar(CRM_CREATED_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(CRM_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(CRM_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(CRM_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         CRM_LAST_MOD_DT = if_else(nchar(CRM_LAST_MOD_DT)==0,as.character(""),
                                   paste(as.character(format(as.Date(CRM_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                         toupper(substring(months(as.Date(CRM_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                         as.character(format(as.Date(CRM_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )

bugsoppty <- bugsoppty %>%
  head(nrow(bugsoppty) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Name,
         BUP_ID = Business.Unit.Plan.ID,
         BUP_PLAN_NM = BU.Plan.Name,
         BUP_BG_NM = BG.Name,
         BUS_ID = BU.Growth.Strategy.ID,
         BUS_NM = BU.Growth.Strategy,
         BUS_TYPE = BU.Growth.Strategy.Type,
         CREATOR = Created.By.Full.Name,
         CREATED_DT = Created.Date,
         OPP_CURRENCY = Currency,
         OPP_LAST_MOD_NM = Last.Modified.By.Full.Name,
         OPP_LAST_MOD_DT = Last.Modified.Date,
         OPP_NM = Opportunity.Opportunity.Name,
         OPP_RBUO_ID = Related.BU.Growth.Opportunities.ID,
         OPP_RBUO_NM = Related.BU.Growth.Opportunities.Name,
         OPP_RBUO_KEY = Unique.Key.RBUO,
         OPP_CLOSE_DT = Opportunity.Close.Date,
         OPP_CREATED_DT = Opportunity.Created.Date,
         OPP_CREATOR = Opportunity.Created.By.Full.Name,
         OPP_ID = Opportunity.Sales.Opportunity.Id,
         OPP_HPSOLUTION = Opportunity.DXC.Solution, # Edited on 4/20. Column name change in SFDC for new company
         OPP_NAME = Opportunity.Opportunity.Name.1,
         OPP_SLS_STAGE = Opportunity.Sales.Stage,
         OPP_FORECAST_CAT = Opportunity.Forecast.Category,
         OPP_GOTOMARKET_ROUTE = Opportunity.Go.To.Market.Route,
         OPP_TTL_VAL_CURRENCY = Opportunity.Total.opportunity.value.Currency,
         OPP_TTL_VALUE = Opportunity.Total.opportunity.value,
         OPP_TTL_CON_CURRENCY = Opportunity.Total.opportunity.value.converted.Currency,
         OPP_TTL_CON = Opportunity.Total.opportunity.value.converted.) %>%
  
  mutate(CREATED_DT = if_else(nchar(CREATED_DT)==0,as.character(""),
                              paste(as.character(format(as.Date(CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                    toupper(substring(months(as.Date(CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                    as.character(format(as.Date(CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         OPP_LAST_MOD_DT = if_else(nchar(OPP_LAST_MOD_DT)==0,as.character(""),
                                   paste(as.character(format(as.Date(OPP_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                         toupper(substring(months(as.Date(OPP_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                         as.character(format(as.Date(OPP_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         OPP_CLOSE_DT = if_else(nchar(OPP_CLOSE_DT)==0,as.character(""),
                                paste(as.character(format(as.Date(OPP_CLOSE_DT, format = '%m/%d/%Y'),"%d")),
                                      toupper(substring(months(as.Date(OPP_CLOSE_DT, format = '%m/%d/%Y')),1,3)),
                                      as.character(format(as.Date(OPP_CLOSE_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         OPP_CREATED_DT = if_else(nchar(OPP_CREATED_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(OPP_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(OPP_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(OPP_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )

bup <- bup %>%
  head(nrow(bup) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Account.Plan.Name,
         BUP_ID = Business.Unit.Plan.ID,
         BUP_PLAN_NM = Business.Unit.Plan.BU.Plan.Name,
         BUP_DETL = Approach.to.growth.and.innovation,
         BUP_BG_NM = BG.Name,
         BUP_EXEC_SPONSOR = BU.Executive.Sponsor,
         BUP_NM = BU.Name,
         BUP_AVG_SCORE = BU.Plan.Average.Score,
         BUP_OWNER = BU.Plan.Owner,
         BUP_PPT_ITG_DESC = BU.Plan.PPT.Integration.Description,
         BUP_STATUS = BU.Plan.Status,
         BUP_COUNTRY = Country,
         BUP_DETL_STAT = Current.happenings.at.the.account,
         BUP_SOW = Current.SOW.,
         BUP_TAM_CURRENCY = Current.TAM.Currency,
         BUP_TAM_AMT = Current.TAM.,
         BUP_TAM_CONV = Current.TAM.converted.Currency,
         BUP_TAM_AMT_CONV = Current.TAM.converted.,
         BUP_FY11_SLS_CURRENCY = FY11.Sales.actuals.Currency,
         BUP_FY11_SLS_AMT = FY11.Sales.actuals.,
         BUP_FY11_SLS_CONV = FY11.Sales.actuals.converted.Currency,
         BUP_FY11_SLS_AMT_CONV = FY11.Sales.actuals.converted.,
         BUP_FY12_SLS_CURRENCY = FY12.Sales.actuals.Currency,
         BUP_FY12_SLS_AMT = FY12.Sales.actuals.,
         BUP_FY12_SLS_CONV = FY12.Sales.actuals.converted.Currency,
         BUP_FY12_SLS_AMT_CONV = FY12.Sales.actuals.converted.,
         BUP_FY13_P_SLS_CURRENCY = FY13.Projected.Sales.Currency,
         BUP_FY13_P_SLS_AMT = FY13.Projected.Sales.,
         BUP_FY13_P_SLS_CONV = FY13.Projected.Sales.converted.Currency,
         BUP_FY13_P_SLS_AMT_CONV = FY13.Projected.Sales.converted.,
         BUP_FY13_A_SLS_CURRENCY = FY13.Sales.actuals.Currency,
         BUP_FY13_A_SLS_AMT = FY13.Sales.actuals.,
         BUP_FY13_A_SLS_CONV = FY13.Sales.actuals.converted.Currency,
         BUP_FY13_A_SLS_AMT_CONV = FY13.Sales.actuals.converted.,
         BUP_FY14_P_SLS_CURRENCY = FY14.Projected.Sales.Currency,
         BUP_FY14_P_SLS_AMT = FY14.Projected.Sales.,
         BUP_FY14_P_SLS_CONV = FY14.Projected.Sales.converted.Currency,
         BUP_FY14_P_SLS_AMT_CONV = FY14.Projected.Sales.converted.,
         BUP_FY14_A_SLS_CURRENCY = FY14.Sales.Currency,
         BUP_FY14_A_SLS_AMT = FY14.Sales.,
         BUP_FY14_A_SLS_CONV = FY14.Sales.converted.Currency,
         BUP_FY14_A_SLS_AMT_CONV = FY14.Sales.converted.,
         BUP_FY15_SLS_CURRENCY = FY15.Sales.Currency,
         BUP_FY15_SLS_AMT = FY15.Sales.,
         BUP_FY15_SLS_CONV = FY15.Sales.converted.Currency,
         BUP_FY15_SLS_AMT_CONV = FY15.Sales.converted.,
         BUP_FY16_SLS_CURRENCY = FY16.Sales.Currency,
         BUP_FY16_SLS_AMT = FY16.Sales.,
         BUP_FY16_SLS_CONV = FY16.Sales.converted.Currency,
         BUP_FY16_SLS_AMT_CONV = FY16.Sales.converted.,
         BUP_FY17_SLS_CURRENCY = FY17.Sales.Currency,
         BUP_FY17_SLS_AMT = FY17.Sales.,
         BUP_FY17_SLS_CONV = FY17.Sales.converted.Currency,
         BUP_FY17_SLS_AMT_CONV = FY17.Sales.converted.,
         BUP_FY18_SLS_CURRENCY = FY18.Sales.Currency,
         BUP_FY18_SLS_AMT = FY18.Sales.,
         BUP_FY18_SLS_CONV = FY18.Sales.converted.Currency,
         BUP_FY18_SLS_AMT_CONV = FY18.Sales.converted.,
         BUP_KEY_CONTACT = Key.Contracts,
         BUP_NOTES = Notes.or.links.to.additional.information,
         BUP_ISSUES = Obstacles.Challenges,
         BUP_Q1 = Q1,
         BUP_Q2 = Q2,
         BUP_Q3 = Q3,
         BUP_Q4 = Q4,
         BUP_Q5 = Q5,
         BUP_REGION = Region,
         BUP_SUBREGION = Sub.Region,
         BUP_YOY1 = Year.over.Year.1,
         BUP_YOY2 = Year.over.Year.2,
         BUP_YOY3 = Year.over.Year.3,
         BUP_YOY4 = Year.over.Year.4,
         BUP_YOY5 = Year.over.Year.5,
         BUP_CURRENCY = Business.Unit.Plan.Currency,
         BUP_CREATOR = Business.Unit.Plan.Created.By,
         BUP_CREATOR_ALIAS = Business.Unit.Plan.Created.Alias,
         BUP_CREATED_DT = Business.Unit.Plan.Created.Date,
         BUP_LAST_MOD_NM = Business.Unit.Plan.Last.Modified.By,
         BUP_LAST_MOD_ALIAS = Business.Unit.Plan.Last.Modified.Alias,
         BUP_LAST_MOD_DT = Business.Unit.Plan.Last.Modified.Date,
         BUP_LAST_ACTIVITY_DT = Business.Unit.Plan.Last.Activity.Date) %>%
  
  mutate(BUP_CREATED_DT = if_else(nchar(BUP_CREATED_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(BUP_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(BUP_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(BUP_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         BUP_LAST_MOD_DT = if_else(nchar(BUP_LAST_MOD_DT)==0,as.character(""),
                                   paste(as.character(format(as.Date(BUP_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                         toupper(substring(months(as.Date(BUP_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                         as.character(format(as.Date(BUP_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         BUP_LAST_ACTIVITY_DT = if_else(nchar(BUP_LAST_ACTIVITY_DT)==0,as.character(""),
                                        paste(as.character(format(as.Date(BUP_LAST_ACTIVITY_DT, format = '%m/%d/%Y'),"%d")),
                                              toupper(substring(months(as.Date(BUP_LAST_ACTIVITY_DT, format = '%m/%d/%Y')),1,3)),
                                              as.character(format(as.Date(BUP_LAST_ACTIVITY_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )

bus <- bus %>%
  head(nrow(bus) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Name,
         BUP_ID = Business.Unit.Plan.ID,
         BUP_NM = BU.Plan.Name,
         BUS_NM = BU.Growth.Strategy,
         BUS_ID = BU.Growth.Strategy.ID,
         BUS_CHALLENGE = Business.Challenge,
         BUS_CREATOR = Created.By.Full.Name,
         BUS_CREATED_DT = Created.Date,
         BUS_CURRENCY = Currency,
         BUS_CUST_IT_PRIOR_ADDRSD = Customer.IT.Priority.addressed,
         BUS_EXECTN_ACT = Execution.Activities,
         BUS_HP_DIFF = How.does.HPE.Differentiate.,
         BUS_COMP_REACT = How.will.the.Competitor.React.,
         BUS_LAST_ACT_DT = Last.Activity.Date,
         BUS_LAST_MOD_NM = Last.Modified.By.Full.Name,
         BUS_LAST_MOD_DT = Last.Modified.Date,
         BUS_PARTN_INFLNCE_STRAT = Partner.Influencer.Strategy,
         BUS_REQ_PROC_CHANGE = Required.Process.Change,
         BUS_TCV_CURRENCY = Sales.Potential.TCV.Currency,
         BUS_TCV_AMT = Sales.Potential.TCV,
         BUS_TCV_CONV = Sales.Potential.TCV.converted.Currency,
         BUS_TCV_AMT_CONV = Sales.Potential.TCV.converted.,
         BUS_SOLUTION_COMPONENTS = Solution.Components,
         BUS_TIMING = Timing,
         BUS_BU_SOLTN = What.is.the.BU.Solution.,
         BUS_TYPE = BU.Growth.Strategy.Type) %>%
  
  mutate(BUS_CREATED_DT = if_else(nchar(BUS_CREATED_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(BUS_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(BUS_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(BUS_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         BUS_LAST_MOD_DT = if_else(nchar(BUS_LAST_MOD_DT)==0,as.character(""),
                                   paste(as.character(format(as.Date(BUS_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                         toupper(substring(months(as.Date(BUS_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                         as.character(format(as.Date(BUS_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )

compls <- compls %>%
  head(nrow(compls) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Account.Plan.Name,
         CL_ID = Competitive.Landscape.ID,
         CL_COMMENTS = Additional.Comments,
         CL_BUS_AREA = Business.Area,
         CL_BUS_GRP = Business.Group,
         CL_BUS_UNIT = Business.Unit,
         CL_COMP1 = Competitor.1,
         CL_COMP1_SOW = Competitor.1.Current.Share.of.Wallet.,
         CL_COMP2 = Competitor.2,
         CL_COMP2_SOW = Competitor.2.Current.Share.of.Wallet.,
         CL_COMP3 = Competitor.3,
         CL_COMP3_SOW = Competitor.3.Current.Share.of.Wallet.,
         CL_HP_SOW = Current.HPE.Share.of.Wallet.,
         CL_NEXT_STEP = Next.Step,
         CL_SUMMARY = Summary.of.Competitive.Landscape,
         CL_CURRENCY = Competitive.Landscape.Currency,
         CL_CREATOR = Competitive.Landscape.Created.By,
         CL_CREATOR_ALIAS = Competitive.Landscape.Created.Alias,
         CL_CREATED_DT = Competitive.Landscape.Created.Date,
         CL_LAST_MOD_NM = Competitive.Landscape.Last.Modified.By,
         CL_LAST_MOD_ALIAS = Competitive.Landscape.Last.Modified.Alias,
         CL_LAST_MOD_DT = Competitive.Landscape.Last.Modified.Date,
         CL_LAST_ACTIVITY_DT = Competitive.Landscape.Last.Activity.Date) %>%
  
  mutate(CL_CREATED_DT = if_else(nchar(CL_CREATED_DT)==0,as.character(""),
                                 paste(as.character(format(as.Date(CL_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                       toupper(substring(months(as.Date(CL_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                       as.character(format(as.Date(CL_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         CL_LAST_MOD_DT = if_else(nchar(CL_LAST_MOD_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(CL_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(CL_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(CL_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         CL_LAST_ACTIVITY_DT = if_else(nchar(CL_LAST_ACTIVITY_DT)==0,as.character(""),
                                       paste(as.character(format(as.Date(CL_LAST_ACTIVITY_DT, format = '%m/%d/%Y'),"%d")),
                                             toupper(substring(months(as.Date(CL_LAST_ACTIVITY_DT, format = '%m/%d/%Y')),1,3)),
                                             as.character(format(as.Date(CL_LAST_ACTIVITY_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )

custprior <- custprior %>%
  head(nrow(custprior) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Account.Plan.Name,
         CBP_ID = Customer.Business.Priority.ID,
         CBP_NM = Customer.Business.Priority.Customer.Business.Priority.Name,
         CBP_DETL_DESC = Detailed.Description,
         CBP_EXP_OUTCOME = Expected.Outcome,
         CBP_PRIOR = Priority,
         CBP_DESC = Short.Description,
         CBP_TIMING = Timing,
         CBP_CURRENCY = Customer.Business.Priority.Currency,
         CBP_CREATOR = Customer.Business.Priority.Created.By,
         CBP_CREATOR_ALIAS = Customer.Business.Priority.Created.Alias,
         CBP_CREATED_DT = Customer.Business.Priority.Created.Date,
         CBP_LAST_MOD_NM = Customer.Business.Priority.Last.Modified.By,
         CBP_LAST_MOD_ALIAS = Customer.Business.Priority.Last.Modified.Alias,
         CBP_LAST_MOD_DT = Customer.Business.Priority.Last.Modified.Date,
         CBP_LAST_ACTIVITY_DT = Customer.Business.Priority.Last.Activity.Date) %>%
  
  mutate(CBP_CREATED_DT = if_else(nchar(CBP_CREATED_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(CBP_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(CBP_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(CBP_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         CBP_LAST_MOD_DT = if_else(nchar(CBP_LAST_MOD_DT)==0,as.character(""),
                                   paste(as.character(format(as.Date(CBP_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                         toupper(substring(months(as.Date(CBP_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                         as.character(format(as.Date(CBP_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         CBP_LAST_ACTIVITY_DT = if_else(nchar(CBP_LAST_ACTIVITY_DT)==0,as.character(""),
                                        paste(as.character(format(as.Date(CBP_LAST_ACTIVITY_DT, format = '%m/%d/%Y'),"%d")),
                                              toupper(substring(months(as.Date(CBP_LAST_ACTIVITY_DT, format = '%m/%d/%Y')),1,3)),
                                              as.character(format(as.Date(CBP_LAST_ACTIVITY_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )

opp <- opp %>%
  head(nrow(opp) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Name,
         HP_SI_ID = HPE.Strategic.Initiative.ID,
         OPP_CLOSE_DT = Opportunity.Close.Date,
         OPP_CREATOR = Opportunity.Created.By.Full.Name,
         OPP_CREATED_DT = Opportunity.Created.Date,
         OPP_ID = Opportunity.Sales.Opportunity.Id,
         OPP_HPSOLUTION = Opportunity.DXC.Solution,
         OPP_NM = Opportunity.Opportunity.Name,
         OPP_OWNER_NM = Opportunity.Opportunity.Owner.Full.Name,
         OPPN_REC_TYPE = Opportunity.Opportunity.Record.Type,
         OPP_SLS_STAGE = Opportunity.Opportunity.Sales.Stage,
         OPP_OWNER = Opportunity.Owner.Full.Name,
         OPP_PRIOR_SLS_STAGE = Opportunity.Prior.Sales.Stage,
         OPP__PRI_PARTN_ACCT = Opportunity.Primary.Partner.Account.Account.Name,
         OPP_PRI_CAMP_NM = Opportunity.Primary.Campaign.Name,
         OPP_PRI_CAMP_SOURCE = Opportunity.Primary.Campaign.Source.Campaign.Name,
         OPP_PRI_CHNLPARTN_ACCT = Opportunity.Primary.Channel.Partner.Account.Name,
         OPP_PRI_COMP = Opportunity.Primary.Competitor.Account.Name,
         OPP_FORECAST_CAT = Opportunity.Forecast.Category,
         OPP_GOTOMARKET_ROUTE = Opportunity.Go.To.Market.Route,
         OPP_CURRENCY = Opportunity.Currency,
         OPP_TTL_VAL_CURRENCY = Opportunity.Total.opportunity.value.Currency,
         OPP_TTL_VALUE = Opportunity.Total.opportunity.value,
         OPP_TTL_CON_CURRENCY = Opportunity.Total.opportunity.value.converted.Currency,
         OPP_TTL_CON = Opportunity.Total.opportunity.value.converted.) %>%
  
  mutate(OPP_CLOSE_DT = if_else(nchar(OPP_CLOSE_DT)==0,as.character(""),
                                paste(as.character(format(as.Date(OPP_CLOSE_DT, format = '%m/%d/%Y'),"%d")),
                                      toupper(substring(months(as.Date(OPP_CLOSE_DT, format = '%m/%d/%Y')),1,3)),
                                      as.character(format(as.Date(OPP_CLOSE_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         OPP_CREATED_DT = if_else(nchar(OPP_CREATED_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(OPP_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(OPP_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(OPP_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )

partnerlandscape <- partnerlandscape %>%
  head(nrow(partnerlandscape) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Name,
         PARTN_ACCT_NM = Alliance.Channel.Partner.Account.Name,
         PARTN_ID = Alliance.Channel.Partner.ID,
         PARTN_ALLIANCE = Alliance.Partner,
         PARTN_CHNNL = Channel.Partner,
         PARTN_CREATOR_NM = Created.By.Full.Name,
         PARTN_CREATED_DT = Created.Date,
         PARTN_CURRENCY = Currency,
         PARTN_EST_REV_AMT = Estimated.Revenue.Potential.to.HPE.M.,
         PARTN_HP_STRAT = HPE.s.Strategy.to.Work.with.the.Partner,
         PARTN_MGR_NM = HPE.Partner.Manager.Full.Name,
         PARTN_LAST_MOD_NM = Last.Modified.By.Full.Name,
         PARTN_LAST_MOD_DT = Last.Modified.Date,
         PARTN_FOCUS = Partner.Focus.Areas.on.the.Account,
         PARTN_INFLUENCE = Partner.Influence.on.Account,
         PARTN_SEQ = Partner.Sequence,
         PARTNER_TYPE = Partner.Type) %>%
  
  mutate(PARTN_CREATED_DT = if_else(nchar(PARTN_CREATED_DT)==0,as.character(""),
                                    paste(as.character(format(as.Date(PARTN_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                          toupper(substring(months(as.Date(PARTN_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                          as.character(format(as.Date(PARTN_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         PARTN_LAST_MOD_DT = if_else(nchar(PARTN_LAST_MOD_DT)==0,as.character(""),
                                     paste(as.character(format(as.Date(PARTN_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                           toupper(substring(months(as.Date(PARTN_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                           as.character(format(as.Date(PARTN_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )

scorecard <- scorecard %>%
  head(nrow(scorecard) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Account.Plan.Name,
         SC_ID = Account.Plan.Scorecard.ID,
         SC_NM = Account.Plan.Scorecard.ABP.Scorecard.Name,
         SC_ACCT_SUMRY_ANSWER = Account.Summary.Answer,
         SC_SUMRY_AVG = Account.Summary.avg,
         SC_SUMRY_COMMENTS = Account.Summary.comments,
         SC_SUMRY_ADTNL = Additional.Comments,
         SC_BUP_A1 = BU.plan.Answer.1,
         SC_BUP_A2 = BU.plan.Answer.2,
         SC_BUP_A3 = BU.plan.Answer.3,
         SC_BUP_A4 = BU.plan.Answer.4,
         SC_BUP_A5 = BU.plan.Answer.5,
         SC_BUP_C1 = BU.plan.Comments.1,
         SC_BUP_ANSWER = Business.Unit.Plans.Answer,
         SC_BUP_COMMENTS = Business.Unit.Plans.comments,
         SC_COMPLS_ANSWER = Competitive.Landscapes.Answer,
         SC_COMPLS_COMMENTS = Competitive.Landscapes.Comments,
         SC_CBP_ANSWER = Customer.Business.Priorities.Answer,
         SC_CBP_AVG = Customer.Business.Priorities.Avg,
         SC_CBP_COMMENTS = Customer.Business.Priorities.comments,
         SC_CR_A1 = Customer.Relationship.Maps.Answer.1,
         SC_CR_A2 = Customer.Relationship.Maps.Answer.2,
         SC_CR_A3 = Customer.Relationship.Maps.Answer.3,
         SC_CR_A4 = Customer.Relationship.Maps.Answer.4,
         SC_CR_COMMENTS1 = Customer.Relationship.Maps.Comments.1,
         SC_CR_COMMENTS3 = Customer.Relationship.Maps.Comments.3,
         SC_CR_COMMENTS4 = Customer.Relationship.Maps.Comments.4,
         SC_CR_COMMENTS2 = Customer.Relationship.Maps.Commets.2,
         SC_CR_AVG = Customer.Rel.Maps.Avg,
         SC_HPSI_AVG = HPSI.Avg,
         SC_HPSI_A1 = HPE.Strategic.Initiatives.Answer.1,
         SC_HPSI_A2 = HPE.Strategic.Initiatives.Answer.2,
         SC_HPSI_COMMENTS1 = HPE.Strategic.Initiatives.Comments.1,
         SC_HPSI_COMMENTS2 = HPE.Strategic.Initiatives.comments.2,
         SC_IA_ANSWER = Innovation.Agenda.Answer,
         SC_IA_COMMENTS = Innovation.Agenda.Comments,
         SC_Q1 = Q1,
         SC_Q10 = Q10,
         SC_Q11 = Q11,
         SC_Q2 = Q2,
         SC_Q3 = Q3,
         SC_Q4 = Q4,
         SC_Q5 = Q5,
         SC_Q6 = Q6,
         SC_Q7 = Q7,
         SC_Q8 = Q8,
         SC_Q9 = Q9,
         SC_TOTAL = Totals,
         SC_ABP_SCORECARD_CURRENCY = Account.Plan.Scorecard.Currency,
         SC_CREATOR = Account.Plan.Scorecard.Created.By,
         SC_CREATED_ALIAS = Account.Plan.Scorecard.Created.Alias,
         SC_CREATED_DT = Account.Plan.Scorecard.Created.Date,
         SC_LAST_MOD_NM = Account.Plan.Scorecard.Last.Modified.By,
         SC_LAST_MOD_ALIAS = Account.Plan.Scorecard.Last.Modified.Alias,
         SC_LAST_MOD_DT = Account.Plan.Scorecard.Last.Modified.Date,
         SC_LAST_ACT_DT = Account.Plan.Scorecard.Last.Activity.Date) %>%
  
  mutate(SC_CREATED_DT = if_else(nchar(SC_CREATED_DT)==0,as.character(""),
                                 paste(as.character(format(as.Date(SC_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                       toupper(substring(months(as.Date(SC_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                       as.character(format(as.Date(SC_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         SC_LAST_MOD_DT = if_else(nchar(SC_LAST_MOD_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(SC_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(SC_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(SC_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         SC_LAST_ACT_DT = if_else(nchar(SC_LAST_ACT_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(SC_LAST_ACT_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(SC_LAST_ACT_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(SC_LAST_ACT_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )

stratinit <- stratinit %>%
  head(nrow(stratinit) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Account.Plan.Name,
         SI_ID = HPE.Strategic.Initiative.ID,
         SI_NM = HPE.Strategic.Initiative.HPE.Strategic.Initiative.Name,
         SI_CUST_BUS_CASE = Customer.Business.Case,
         SI_CUST_BUS_PRIOR = Customer.business.priority.addressed,
         SI_CUST_SPONSOR = Customer.Sponsor.Name,
         SI_DESC = Description.of.the.Initiative,
         SI_EXECTN_TIMEFRAME = Execution.Timeframe,
         SI_EXECTN_STAT = HPE.Execution.Status,
         SI_FUNDG_STAT = HPE.Funding.Status,
         SI_SOLUTION = HPE.Solutions,
         SI_HP_SPONSOR_NM = HPE.Sponsor.Name,
         SI_INNOV_INIT = Innovation.Initiative,
         SI_NON_DISCLSR_AGRMT = Non.Disclosure.Agreement,
         SI_PROJ_INIT_CUR = Projected.Initiative.HPE.Value.Currency,
         SI_PROJ_INIT_VAL = Projected.Initiative.HPE.Value.,
         SI_PROJ_INIT_CONV = Projected.Initiative.HPE.Value.converted.Currency,
         SI_PROJ_INIT_VAL_CONV = Projected.Initiative.HPE.Value.converted.,
         SI_DEPENDENCIES = Summary.of.Dependencies,
         SI_RISK = Summary.of.Risks.and.Issues,
         SI_CURRENCY = HPE.Strategic.Initiative.Currency,
         SI_CREATOR_NM = HPE.Strategic.Initiative.Created.By,
         SI_CREATOR_ALIAS = HPE.Strategic.Initiative.Created.Alias,
         SI_CREATED_DT = HPE.Strategic.Initiative.Created.Date,
         SI_LAST_MOD_NM = HPE.Strategic.Initiative.Last.Modified.By,
         SI_LAST_MOD_ALIAS = HPE.Strategic.Initiative.Last.Modified.Alias,
         SI_LAST_MOD_DT = HPE.Strategic.Initiative.Last.Modified.Date,
         SI_LAST_ACTIVITY_DT  = HPE.Strategic.Initiative.Last.Activity.Date) %>%
  
  mutate(SI_INNOV_INIT = as.character(SI_INNOV_INIT),
         SI_NON_DISCLSR_AGRMT = as.character(SI_NON_DISCLSR_AGRMT),
         
         SI_CREATED_DT = if_else(nchar(SI_CREATED_DT)==0,as.character(""),
                                 paste(as.character(format(as.Date(SI_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                       toupper(substring(months(as.Date(SI_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                       as.character(format(as.Date(SI_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         SI_LAST_MOD_DT = if_else(nchar(SI_LAST_MOD_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(SI_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(SI_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(SI_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         SI_LAST_ACTIVITY_DT = if_else(nchar(SI_LAST_ACTIVITY_DT)==0,as.character(""),
                                       paste(as.character(format(as.Date(SI_LAST_ACTIVITY_DT, format = '%m/%d/%Y'),"%d")),
                                             toupper(substring(months(as.Date(SI_LAST_ACTIVITY_DT, format = '%m/%d/%Y')),1,3)),
                                             as.character(format(as.Date(SI_LAST_ACTIVITY_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )

tce <- tce %>%
  head(nrow(tce) - 7) %>%
  mutate(SNAP_DT = current_day) %>%
  select(SNAP_DT,
         ACCT_PLAN_ID = Account.Plan.ID,
         ACCT_PLAN_NM = Account.Plan.Name,
         TCE_ADDTNL_INFO = Additional.Information,
         TCE_CREATOR = Created.By.Full.Name,
         TCE_CREATED_DT = Created.Date,
         TCE_CURRENCY = Currency,
         TCE_CUR_SCORE = Current.Year.Score,
         TCE_METRICS = HPE.TCE.Metrics,
         TCE_LAST_ACT_DT = Last.Activity.Date,
         TCE_LAST_MOD_NM = Last.Modified.By.Full.Name,
         TCE_LAST_MOD_DT = Last.Modified.Date,
         TCE_CUST_CONCERNS = Main.Customer.Concerns.Issues,
         TCE_PREV_SCORE = Previous.Year.Score,
         TCE_STATUS = Status,
         TCE_STRATEGY = Strategy.to.Address.Concerns.Issues,
         TCE_CODE = Total.Customer.Experience,
         TCE_ID = Total.Customer.Experience.ID) %>%
  
  mutate(TCE_CREATED_DT = if_else(nchar(TCE_CREATED_DT)==0,as.character(""),
                                  paste(as.character(format(as.Date(TCE_CREATED_DT, format = '%m/%d/%Y'),"%d")),
                                        toupper(substring(months(as.Date(TCE_CREATED_DT, format = '%m/%d/%Y')),1,3)),
                                        as.character(format(as.Date(TCE_CREATED_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         TCE_LAST_ACT_DT = if_else(nchar(TCE_LAST_ACT_DT)==0,as.character(""),
                                   paste(as.character(format(as.Date(TCE_LAST_ACT_DT, format = '%m/%d/%Y'),"%d")),
                                         toupper(substring(months(as.Date(TCE_LAST_ACT_DT, format = '%m/%d/%Y')),1,3)),
                                         as.character(format(as.Date(TCE_LAST_ACT_DT, format = '%m/%d/%Y'),"%Y")), sep="")),
         
         TCE_LAST_MOD_DT = if_else(nchar(TCE_LAST_MOD_DT)==0,as.character(""),
                                   paste(as.character(format(as.Date(TCE_LAST_MOD_DT, format = '%m/%d/%Y'),"%d")),
                                         toupper(substring(months(as.Date(TCE_LAST_MOD_DT, format = '%m/%d/%Y')),1,3)),
                                         as.character(format(as.Date(TCE_LAST_MOD_DT, format = '%m/%d/%Y'),"%Y")), sep=""))
  )


# Save weekly extract to RDS data
saveRDS(abp, "abp.RDS")
saveRDS(acct, "acct.RDS")
saveRDS(acctcustomercontact, "acctcustomercontact.RDS")
saveRDS(bugsoppty, "bugsoppty.RDS")
saveRDS(bup, "bup.RDS")
saveRDS(bus, "bus.RDS")
saveRDS(compls, "compls.RDS")
saveRDS(custprior, "custprior.RDS")
saveRDS(opp, "opp.RDS")
saveRDS(partnerlandscape, "partnerlandscape.RDS")
saveRDS(scorecard, "scorecard.RDS")
saveRDS(stratinit, "stratinit.RDS")
saveRDS(tce, "tce.RDS")


# abp                 <- readRDS("abp.RDS")
# acct                <- readRDS("acct.RDS")
# acctcustomercontact <- readRDS("acctcustomercontact.RDS")
# bugsoppty           <- readRDS("bugsoppty.RDS")
# bup                 <- readRDS("bup.RDS")
# bus                 <- readRDS("bus.RDS")
# compls              <- readRDS("compls.RDS")
# custprior           <- readRDS("custprior.RDS")
# opp                 <- readRDS("opp.RDS")
# partnerlandscape    <- readRDS("partnerlandscape.RDS")
# scorecard           <- readRDS("scorecard.RDS")
# stratinit           <- readRDS("stratinit.RDS")
# tce                 <- readRDS("tce.RDS")


################################################################################
######################### Step 3 Append to Master Data #########################
################################################################################

setwd("/home/zhaoch/R_Workspace/Acct Growth/Data/arc")


Load_Master <- function(mydata) {
  
  data_name <- paste0(deparse(substitute(mydata)), ".RDS")
  
  arc <- readRDS(data_name)
  
  # Clean Master Dataset to avoid duplicate if running the code again
  arc <- arc %>% filter(SNAP_DT != current_day)
  
  arc
}


# Call function to Load Master Dataset
abp.arc                 <- Load_Master(abp)
acct.arc                <- Load_Master(acct)
acctcustomercontact.arc <- Load_Master(acctcustomercontact)
bugsoppty.arc           <- Load_Master(bugsoppty)
bup.arc                 <- Load_Master(bup)
bus.arc                 <- Load_Master(bus)
compls.arc              <- Load_Master(compls)
custprior.arc           <- Load_Master(custprior)
opp.arc                 <- Load_Master(opp)
partnerlandscape.arc    <- Load_Master(partnerlandscape)
scorecard.arc           <- Load_Master(scorecard)
stratinit.arc           <- Load_Master(stratinit)
tce.arc                 <- Load_Master(tce)


# Clean Master Dataset to avoid duplicate if running the code again
abp.arc                 <- abp.arc                 %>% filter(SNAP_DT != current_day)
acct.arc                <- acct.arc                %>% filter(SNAP_DT != current_day)
acctcustomercontact.arc <- acctcustomercontact.arc %>% filter(SNAP_DT != current_day)
bugsoppty.arc           <- bugsoppty.arc           %>% filter(SNAP_DT != current_day)
bup.arc                 <- bup.arc                 %>% filter(SNAP_DT != current_day)
bus.arc                 <- bus.arc                 %>% filter(SNAP_DT != current_day)
compls.arc              <- compls.arc              %>% filter(SNAP_DT != current_day)
custprior.arc           <- custprior.arc           %>% filter(SNAP_DT != current_day)
opp.arc                 <- opp.arc                 %>% filter(SNAP_DT != current_day)
partnerlandscape.arc    <- partnerlandscape.arc    %>% filter(SNAP_DT != current_day)
scorecard.arc           <- scorecard.arc           %>% filter(SNAP_DT != current_day)
stratinit.arc           <- stratinit.arc           %>% filter(SNAP_DT != current_day)
tce.arc                 <- tce.arc                 %>% filter(SNAP_DT != current_day)


# Append weekly SFDC extract to Master Dataset
abp                 <- bind_rows(abp.arc, abp)
acct                <- bind_rows(acct.arc, acct)
acctcustomercontact <- bind_rows(acctcustomercontact.arc, acctcustomercontact)
bugsoppty           <- bind_rows(bugsoppty.arc, bugsoppty)
bup                 <- bind_rows(bup.arc, bup)
bus                 <- bind_rows(bus.arc, bus)
compls              <- bind_rows(compls.arc, compls)
custprior           <- bind_rows(custprior.arc, custprior)
opp                 <- bind_rows(opp.arc, opp)
partnerlandscape    <- bind_rows(partnerlandscape.arc, partnerlandscape)
scorecard           <- bind_rows(scorecard.arc, scorecard)
stratinit           <- bind_rows(stratinit.arc, stratinit)
tce                 <- bind_rows(tce.arc, tce)


# Need function here to test # of variables change !!!


# Clean Work Space
rm(abp.arc)
rm(acct.arc)
rm(acctcustomercontact.arc)
rm(bugsoppty.arc)
rm(bup.arc)
rm(bus.arc)
rm(compls.arc)
rm(custprior.arc)
rm(opp.arc)
rm(partnerlandscape.arc)
rm(scorecard.arc)
rm(stratinit.arc)
rm(tce.arc)


################################################################################
###################### Step 4 Calculate the Derived Fields #####################
################################################################################

abp <- abp %>%
  mutate(score1 = if_else(is.na(nchar(gsub("\\s+"," ",iconv(ABP_CUST_STRAT, "UTF-8", "ASCII", sub = "")))),0.1,
                          if_else(nchar(gsub("\\s+"," ",iconv(ABP_CUST_STRAT, "UTF-8", "ASCII", sub = "")))>500,0.25,
                                  if_else(nchar(gsub("\\s+"," ",iconv(ABP_CUST_STRAT, "UTF-8", "ASCII", sub = "")))>=200,0.15,0.1))),
         
         score2 = if_else(is.na(nchar(gsub("\\s+"," ",iconv(ABP_HP_CUST_STATE, "UTF-8", "ASCII", sub = "")))),0.1,
                          if_else(nchar(gsub("\\s+"," ",iconv(ABP_HP_CUST_STATE, "UTF-8", "ASCII", sub = "")))>500,0.25,
                                  if_else(nchar(gsub("\\s+"," ",iconv(ABP_HP_CUST_STATE, "UTF-8", "ASCII", sub = "")))>=200,0.15,0.1))),
         
         score3 = if_else(is.na(nchar(gsub("\\s+"," ",iconv(ABP_STRAT_OPP, "UTF-8", "ASCII", sub = "")))),0.1,
                          if_else(nchar(gsub("\\s+"," ",iconv(ABP_STRAT_OPP, "UTF-8", "ASCII", sub = "")))>500,0.25,
                                  if_else(nchar(gsub("\\s+"," ",iconv(ABP_STRAT_OPP, "UTF-8", "ASCII", sub = "")))>=200,0.15,0.1))),
         
         score4 = if_else(is.na(nchar(gsub("\\s+"," ",iconv(ABP_KEY_ISSUE, "UTF-8", "ASCII", sub = "")))),0.1,
                          if_else(nchar(gsub("\\s+"," ",iconv(ABP_KEY_ISSUE, "UTF-8", "ASCII", sub = "")))>500,0.25,
                                  if_else(nchar(gsub("\\s+"," ",iconv(ABP_KEY_ISSUE, "UTF-8", "ASCII", sub = "")))>=200,0.15,0.1))),
         
         ABP_COMPL_SCORE = round(score1 + score2 + score3 + score4, 2),
         
         ABP_EXEC_ASKS_FLAG = if_else(ABP_EXEC_ASKS != '',"Yes","NO"),
         
         score5 = if_else(is.na(nchar(gsub("\\s+"," ",iconv(ABP_CUST_DFN_INNVTN, "UTF-8", "ASCII", sub = "")))),0.1,
                          if_else(nchar(gsub("\\s+"," ",iconv(ABP_CUST_DFN_INNVTN, "UTF-8", "ASCII", sub = "")))>400,0.3,
                                  if_else(nchar(gsub("\\s+"," ",iconv(ABP_CUST_DFN_INNVTN, "UTF-8", "ASCII", sub = "")))>=200,0.2,0.1))),
         
         score6 = if_else(is.na(nchar(gsub("\\s+"," ",iconv(ABP_HP_INNVTN_STRAT, "UTF-8", "ASCII", sub = "")))),0.1,
                          if_else(nchar(gsub("\\s+"," ",iconv(ABP_HP_INNVTN_STRAT, "UTF-8", "ASCII", sub = "")))>400,0.3,
                                  if_else(nchar(gsub("\\s+"," ",iconv(ABP_HP_INNVTN_STRAT, "UTF-8", "ASCII", sub = "")))>=200,0.2,0.1))),
         
         score7 = if_else(is.na(as.numeric(as.Date(ABP_LAST_CUST_ENGGMNT, "%d%b%Y") - as.Date(SNAP_DT,"%d%b%Y"))),0,
                          if_else(as.numeric(as.Date(ABP_LAST_CUST_ENGGMNT, "%d%b%Y") - as.Date(SNAP_DT,"%d%b%Y"))>90,0.4,0)),
         
         ABP_IA_COMPL_SCORE = round(score5 + score6 + score7, 2),
         
         ABP_LAST_CUST_ENGGMNT_FLAG = 
           if_else(nchar(ABP_LAST_CUST_ENGGMNT)==0, "Missing",
                   if_else(as.numeric(format(as.Date(SNAP_DT, "%d%b%Y"), "%m")) - as.numeric(format(as.Date(ABP_LAST_CUST_ENGGMNT, "%d%b%Y"), "%m"))>9, "Red",
                           if_else(as.numeric(format(as.Date(SNAP_DT, "%d%b%Y"), "%m")) - as.numeric(format(as.Date(ABP_LAST_CUST_ENGGMNT, "%d%b%Y"), "%m"))>6, "Amber", "Green"))),
         
         ABP_NEXT_CUST_ENGGMNT_FLAG = 
           if_else(nchar(ABP_NEXT_CUST_ENGGMNT)==0, "Missing",
                   if_else(nchar(ABP_LAST_CUST_ENGGMNT)!=0 &  
                             as.numeric(format(as.Date(ABP_NEXT_CUST_ENGGMNT, "%d%b%Y"), "%m")) - as.numeric(format(as.Date(ABP_LAST_CUST_ENGGMNT, "%d%b%Y"), "%m"))>6, "Red",
                           if_else(as.numeric(format(as.Date(ABP_NEXT_CUST_ENGGMNT, "%d%b%Y"), "%m")) - as.numeric(format(as.Date(SNAP_DT, "%d%b%Y"), "%m"))>6, "Amber", 
                                   if_else(nchar(ABP_LAST_CUST_ENGGMNT)!=0 & 
                                             as.numeric(format(as.Date(ABP_NEXT_CUST_ENGGMNT, "%d%b%Y"), "%m")) - as.numeric(format(as.Date(ABP_LAST_CUST_ENGGMNT, "%d%b%Y"), "%m"))>3, "Amber",
                                           if_else(as.numeric(format(as.Date(ABP_NEXT_CUST_ENGGMNT, "%d%b%Y"), "%m")) - as.numeric(format(as.Date(SNAP_DT, "%d%b%Y"), "%m"))>=0, "Green",
                                                   if_else(nchar(ABP_LAST_CUST_ENGGMNT)!=0 & 
                                                             as.numeric(format(as.Date(ABP_NEXT_CUST_ENGGMNT, "%d%b%Y"), "%m")) - as.numeric(format(as.Date(ABP_LAST_CUST_ENGGMNT, "%d%b%Y"), "%m"))>=0, "Green",
                                                           if_else(as.numeric(format(as.Date(ABP_NEXT_CUST_ENGGMNT, "%d%b%Y"), "%m")) - as.numeric(format(as.Date(SNAP_DT, "%d%b%Y"), "%m"))<0, "N/A",
                                                                   if_else(nchar(ABP_LAST_CUST_ENGGMNT)!=0 | 
                                                                             as.numeric(format(as.Date(ABP_NEXT_CUST_ENGGMNT, "%d%b%Y"), "%m")) - as.numeric(format(as.Date(ABP_LAST_CUST_ENGGMNT, "%d%b%Y"), "%m"))<0, "N/A",
                                                                           "Other")))))))),
         
         ABP_CUST_DFNTN_AGRD_FLAG = if_else(ABP_CUST_DFNTN_AGRD==1, "Y", "N"),
         
         ABP_HP_CUST_GOVN_FLAG = if_else(nchar(iconv(ABP_HP_CUST_GOVN, to = "ASCII", sub = ""))==0, "Y", "N"),
         
         ABP_INNVTN_ENGGMNT_REF_FLAG = if_else(nchar(iconv(ABP_INNVTN_ENGGMNT_REF, to = "ASCII", sub = ""))==0, "Y", "N"),
         
         ABP_MTH_SINCE_LAST_MOD = as.numeric(format(as.Date(SNAP_DT, "%d%b%Y"), "%m")) - 
           as.numeric(format(as.Date(ABP_LAST_CUST_ENGGMNT, "%d%b%Y"), "%m")),
         
         
         # ES/EG - led based on abp owner role dated on 12/18/2013;
         
         OWNER = substr(word(ABP_OWNER_ROLE, 1), 1, 200),
         
         ABP_IND_SEG = if_else(ACCT_ID == "001G000000mkDWf", "OneHP", ABP_IND_SEG),
         
         ABP_PLAN_STATUS = if_else(substring(ABP_PLAN_STATUS,1,9) == "Submitted", "Submitted/Pending Review", ABP_PLAN_STATUS)
  ) %>%
  select(-starts_with("score"))


# region/subregion/country for acct
acct <- acct %>%
  mutate(ACCT_SUBREG1 = if_else(word(ACCT_GEO_HIERARCHY2, 1, sep=";")=="Americas","AMS",
                                if_else(word(ACCT_GEO_HIERARCHY2, 1, sep=";")=="EMEA","EMEA",
                                        if_else(word(ACCT_GEO_HIERARCHY2, 1, sep=";")=="Asia Pacific","APJ",
                                                if_else(word(ACCT_GEO_HIERARCHY2, 1, sep=";")=="SEA", "APJ", " ")))),
         
         ACCT_SUBREG2 = if_else(ACCT_ID=="001G000000mkDWf", "OneHP",
                                if_else(ACCT_REG=="SEA", "AP without Japan", word(ACCT_GEO_HIERARCHY2, 2, sep=";"))),
         
         ACCT_SUBREG3 = if_else(ACCT_ID=="001G000000mkDWf", "OneHP", 
                                word(ACCT_GEO_HIERARCHY2, -2, sep=";"))) %>%
  
  mutate(ACCT_SUBREG2 = if_else(ACCT_SUBREG3=="Japan", "Japan",
                                if_else(ACCT_SUBREG2 %in% c("APJeC", "China"), "AP without Japan", ACCT_SUBREG2)))
  # mutate(ACCT_NM = if_else(ACCT_ID=="0015000001I8hFE", "ZAKAD UBEZPIECZE SPOECZNYCH", 
  #                          if_else(ACCT_ID=="0015000001I8oNN", "CALIFORNIA OFFICE OF SYSTEMS INTEGRATION  CMIPS",
  #                          if_else(ACCT_ID=="0015000001I8mQI", "Ministerstvo financ esk republiky",
  #                          if_else(ACCT_ID=="0015000001I8uyx", "Magyar Telekom Tvkzlsi Nyilvnosan Mkd Rszvnytrsasg",
  #                          if_else(ACCT_ID=="0015000001I8leU", "BB-Business Competence Center GmbH",  
  #                          if_else(ACCT_ID=="0015000001I8aSy", "MOL Magyar Olaj- s Gzipari Nyilvnosan Mkd Rszvnytrsasg",
  #                          if_else(ACCT_ID=="0015000001I8dER", "Magyar Posta Zrtkren Mkd Rszvnytrsasg",  
  #                          if_else(ACCT_ID=="0015000001I8hsO", "POLSKA WYTWRNIA PAPIERW WARTOCIOWYCH S A",  
  #                          if_else(ACCT_ID=="0015000001I8ntP", "Szerencsejtk Zrtkren Mkd Rszvnytrsasg",
  #                                  ACCT_NM)))))))))) # Temp solution to fix arrow


# completeness for bup
bup <- bup %>%
  mutate(BUP_NOTES_LEN = nchar(trimws(iconv(BUP_NOTES, to = "ASCII", sub = ""))),
         BUP_DETL_LEN = nchar(trimws(iconv(BUP_DETL, to = "ASCII", sub = ""))),
         BUP_ISSUES_LEN = nchar(trimws(iconv(BUP_ISSUES, to = "ASCII", sub = ""))),
         BUP_DETL_STAT_LEN = nchar(trimws(iconv(BUP_DETL_STAT, to = "ASCII", sub = ""))),
         
         BUP_SOW_SCORE = if_else(is.na(BUP_SOW), 0, 100),
         BUP_TAM_AMT_SCORE = if_else(is.na(BUP_TAM_AMT_CONV), 0, 100),
         
         BUP_FY11_SLS_AMT_SCORE = if_else(is.na(BUP_FY11_SLS_AMT_CONV), 0, 100),
         
         BUP_FY12_SLS_AMT_SCORE = if_else(is.na(BUP_FY12_SLS_AMT_CONV), 0, 100),
         
         # Edited on Feb/06/2015 to match new BUP template
         BUP_FY13_P_SLS_AMT_SCORE = if_else(is.na(BUP_FY13_P_SLS_AMT_CONV), 0, 100),
         BUP_FY13_A_SLS_AMT_SCORE = if_else(is.na(BUP_FY13_A_SLS_AMT_CONV), 0, 100),
         
         # Edited on Feb/06/2015 to match new BUP template
         BUP_FY14_P_SLS_AMT_SCORE = if_else(is.na(BUP_FY14_P_SLS_AMT_CONV), 0, 100),
         BUP_FY14_A_SLS_AMT_SCORE = if_else(is.na(BUP_FY14_A_SLS_AMT_CONV), 0, 100),
         
         BUP_FY15_SLS_AMT_SCORE = if_else(is.na(BUP_FY15_SLS_AMT_CONV), 0, 100),
         
         BUP_FY16_SLS_AMT_SCORE = if_else(is.na(BUP_FY16_SLS_AMT_CONV), 0, 100),
         
         BUP_FY17_SLS_AMT_SCORE = if_else(is.na(BUP_FY17_SLS_AMT_CONV), 0, 100),
         
         # Added on Feb/06/2015 to match new BUP template
         BUP_FY18_SLS_AMT_SCORE = if_else(is.na(BUP_FY18_SLS_AMT_CONV), 0, 100)
  ) %>%
  arrange(SNAP_DT) %>%
  group_by(SNAP_DT) %>%
  
  mutate(BUP_NOTES_SCORE = floor(rank(BUP_NOTES_LEN)*100/(length(BUP_NOTES_LEN) + 1)),
         
         BUP_DETL_SCORE = floor(rank(BUP_DETL_LEN)*100/(length(BUP_DETL_LEN) + 1)),
         
         BUP_ISSUES_SCORE = floor(rank(BUP_ISSUES_LEN)*100/(length(BUP_ISSUES_LEN) + 1)),
         
         BUP_DETL_STAT_SCORE = floor(rank(BUP_DETL_STAT_LEN)*100/(length(BUP_DETL_STAT_LEN) + 1))
  )


# completeness for bus
bus <- bus %>% 
  mutate(BUS_BU_SOLTN_LEN      = nchar(trimws(iconv(BUS_BU_SOLTN, to = "ASCII", sub = ""))),
         BUS_CHALLENGE_LEN     = nchar(trimws(iconv(BUS_CHALLENGE, to = "ASCII", sub = ""))),
         BUS_CUST_IT_PRIOR_LEN = nchar(trimws(iconv(BUS_CUST_IT_PRIOR_ADDRSD, to = "ASCII", sub = ""))),
         BUS_REQ_PROC_CHG_LEN  = nchar(trimws(iconv(BUS_REQ_PROC_CHANGE, to = "ASCII", sub = ""))),
         BUS_EXECTN_ACT_LEN    = nchar(trimws(iconv(BUS_EXECTN_ACT, to = "ASCII", sub = ""))),
         BUS_HP_DIFF_LEN       = nchar(trimws(iconv(BUS_HP_DIFF, to = "ASCII", sub = ""))),
         BUS_COMP_REACT_LEN    = nchar(trimws(iconv(BUS_COMP_REACT, to = "ASCII", sub = ""))),
         BUS_SOLTN_COMP_LEN    = nchar(trimws(iconv(BUS_SOLUTION_COMPONENTS, to = "ASCII", sub = ""))),
         BUS_PARTN_STRAT_LEN   = nchar(trimws(iconv(BUS_PARTN_INFLNCE_STRAT, to = "ASCII", sub = ""))),
         
         BUS_TCV_AMT_SCORE = if_else(is.na(BUS_TCV_AMT_CONV), 0, 100)) %>% 
  arrange(SNAP_DT) %>%
  group_by(SNAP_DT) %>%
  
  mutate(BUS_BU_SOLTN_SCORE      = floor(rank(BUS_BU_SOLTN_LEN)*100/(length(BUS_BU_SOLTN_LEN) + 1)),
         BUS_CHALLENGE_SCORE     = floor(rank(BUS_CHALLENGE_LEN)*100/(length(BUS_CHALLENGE_LEN) + 1)),
         BUS_CUST_IT_PRIOR_SCORE = floor(rank(BUS_CUST_IT_PRIOR_LEN)*100/(length(BUS_CUST_IT_PRIOR_LEN) + 1)),
         BUS_REQ_PROC_CHG_SCORE  = floor(rank(BUS_REQ_PROC_CHG_LEN)*100/(length(BUS_REQ_PROC_CHG_LEN) + 1)),
         BUS_EXECTN_ACT_SCORE    = floor(rank(BUS_EXECTN_ACT_LEN)*100/(length(BUS_EXECTN_ACT_LEN) + 1)),
         BUS_HP_DIFF_SCORE       = floor(rank(BUS_HP_DIFF_LEN)*100/(length(BUS_HP_DIFF_LEN) + 1)),
         BUS_COMP_REACT_SCORE    = floor(rank(BUS_COMP_REACT_LEN)*100/(length(BUS_COMP_REACT_LEN) + 1)),
         BUS_SOLTN_COMP_SCORE    = floor(rank(BUS_SOLTN_COMP_LEN)*100/(length(BUS_SOLTN_COMP_LEN) + 1)),
         BUS_PARTN_STRAT_SCORE   = floor(rank(BUS_PARTN_STRAT_LEN)*100/(length(BUS_PARTN_STRAT_LEN) + 1))
  )


# seven practice indicators based on Strategy Initiatives
stratinit <- stratinit %>%
  mutate(SI_MOBILITY_WORKPLACE = if_else(grepl("Mobility", iconv(SI_SOLUTION, to = "ASCII", sub = "")) | grepl("mobil", tolower(iconv(SI_NM, to = "ASCII", sub = ""))) | 
                                           grepl("workplace", tolower(iconv(SI_NM, to = "ASCII", sub = ""))), "Y", "N"),
         
         SI_WORKLOAD_CLOUD = if_else(grepl("Converged Cloud", iconv(SI_SOLUTION, to = "ASCII", sub = "")) | grepl("IT Performance Suite", iconv(SI_SOLUTION, to = "ASCII", sub = "")) |
                                       grepl("Converged Infrastructure", iconv(SI_SOLUTION, to = "ASCII", sub = "")) | grepl("mobil", tolower(iconv(SI_NM, to = "ASCII", sub = ""))) | 
                                       grepl("workplace", tolower(iconv(SI_NM, to = "ASCII", sub = ""))), "Y", "N"),
         
         SI_ANALYTICS_DATA_MGMT = if_else(grepl("Big Data", iconv(SI_SOLUTION, to = "ASCII", sub = "")) | grepl("big data", tolower(iconv(SI_NM, to = "ASCII", sub = ""))) | 
                                            grepl("analytic", tolower(iconv(SI_NM, to = "ASCII", sub = ""))) | grepl("data management", tolower(iconv(SI_NM, to = "ASCII", sub = ""))), "Y", "N"),
         
         SI_APP_PROJ_SERVS = if_else(grepl("Application Transformation", iconv(SI_SOLUTION, to = "ASCII", sub = "")) | grepl("Information Optimization", iconv(SI_SOLUTION, to = "ASCII", sub = "")) | 
                                       grepl("transformation", tolower(iconv(SI_NM, to = "ASCII", sub = ""))) | grepl("project", tolower(iconv(SI_NM, to = "ASCII", sub = ""))) | 
                                       grepl("application", tolower(iconv(SI_NM, to = "ASCII", sub = ""))), "Y", "N"),
         
         SI_ENTERPRISE_SECURITY = if_else(grepl("Security and Risk Management", iconv(SI_SOLUTION, to = "ASCII", sub = "")) | grepl("security", tolower(iconv(SI_NM, to = "ASCII", sub = ""))) | 
                                            grepl("risk", tolower(iconv(SI_NM, to = "ASCII", sub = ""))), "Y", "N"),
         
         SI_BUS_PROCESS_SERVS = if_else(grepl("Managed Print Services", iconv(SI_SOLUTION, to = "ASCII", sub = "")) | grepl("bpo", tolower(iconv(SI_NM, to = "ASCII", sub = ""))) | 
                                          grepl("bps", tolower(iconv(SI_NM, to = "ASCII", sub = ""))) | grepl("business process", tolower(iconv(SI_NM, to = "ASCII", sub = ""))), "Y", "N"),
         
         SI_INDUSTRY_SOLUTION = if_else(grepl("industry", tolower(iconv(SI_NM, to = "ASCII", sub = ""))), "Y", "N")
  )


# opp
opp_temp <- opp %>%
  mutate(OPP_SLS_STAGE = if_else(nchar(iconv(OPP_SLS_STAGE, to = "ASCII", sub = ""))==0, "00", OPP_SLS_STAGE),
         OPP_TTL_CON_S0 = 0) %>%
  arrange(HP_SI_ID, SNAP_DT)

stra_temp <- stratinit %>%
  select(HP_SI_ID = SI_ID, 
         SNAP_DT,
         OPP_TTL_CON_S0 = SI_PROJ_INIT_VAL_CONV) %>%
  arrange(HP_SI_ID, SNAP_DT) %>%
  distinct(.keep_all=TRUE)

# HP_SI_ID, SNAP_DT, OPP_TTL_CON_S0

opp_final <- merge(opp_temp, stra_temp, all.x = TRUE)

opp <- opp_final %>%
  mutate(OPP_TTL_CON_S0 = if_else(is.na(OPP_TTL_CON_S0), 0, 
                                  if_else(OPP_SLS_STAGE != "00", 0, OPP_TTL_CON_S0))
  )


## completeness of the fields;
# for si;
stratinit <- stratinit %>%
  mutate(SI_DESC_LEN          = nchar(trimws(iconv(SI_DESC, to = "ASCII", sub = ""))),
         SI_SOLUTION_LEN      = nchar(trimws(iconv(SI_SOLUTION, to = "ASCII", sub = ""))),
         SI_CUST_BUS_CASE_LEN = nchar(trimws(iconv(SI_CUST_BUS_CASE, to = "ASCII", sub = ""))),
         SI_DEPENDENCIES_LEN  = nchar(trimws(iconv(SI_DEPENDENCIES, to = "ASCII", sub = ""))),
         SI_RISK_LEN          = nchar(trimws(iconv(SI_RISK, to = "ASCII", sub = ""))),
         
         SI_PROJ_INIT_VAL_SCORE = if_else(is.na(SI_PROJ_INIT_VAL_CONV), 0, 100)) %>% 
  arrange(SNAP_DT) %>%
  group_by(SNAP_DT) %>%
  
  mutate(SI_DESC_SCORE          = floor(rank(SI_DESC_LEN)*100/(length(SI_DESC_LEN) + 1)),
         SI_SOLUTION_SCORE      = floor(rank(SI_SOLUTION_LEN)*100/(length(SI_SOLUTION_LEN) + 1)),
         SI_CUST_BUS_CASE_SCORE = floor(rank(SI_CUST_BUS_CASE_LEN)*100/(length(SI_CUST_BUS_CASE_LEN) + 1)),
         SI_DEPENDENCIES_SCORE  = floor(rank(SI_DEPENDENCIES_LEN)*100/(length(SI_DEPENDENCIES_LEN) + 1)),
         SI_RISK_SCORE          = floor(rank(SI_RISK_LEN)*100/(length(SI_RISK_LEN) + 1))
  )


# completeness for oppty
opp <- opp %>%
  mutate(OPP_PRI_CAMP_NM_LEN  = nchar(trimws(iconv(OPP_PRI_CAMP_NM, to = "ASCII", sub = ""))),
         OPP_PRI_CAMP_SRC_LEN = nchar(trimws(iconv(OPP_PRI_CAMP_SOURCE, to = "ASCII", sub = ""))),
         OPP_HPSOLUTION_LEN   = nchar(trimws(iconv(OPP_HPSOLUTION, to = "ASCII", sub = ""))),
         OPP_PRI_PARTN_LEN    = nchar(trimws(iconv(OPP__PRI_PARTN_ACCT, to = "ASCII", sub = ""))),
         OPP_PRI_COMP_LEN     = nchar(trimws(iconv(OPP_PRI_COMP, to = "ASCII", sub = ""))),
         
         OPP_TTL_VAL_SCORE  = if_else(is.na(OPP_TTL_CON), 0, 100),
         
         OPP_PRI_COMP_SCORE = if_else(is.na(OPP_PRI_COMP), 0, 100),
         
         OPP_FORECAST_SCORE = if_else(is.na(OPP_FORECAST_CAT), 0, 100)) %>% 
  arrange(SNAP_DT) %>%
  group_by(SNAP_DT) %>%
  
  mutate(OPP_PRI_CAMP_NM_SCORE  = floor(rank(OPP_PRI_CAMP_NM_LEN)*100/(length(OPP_PRI_CAMP_NM_LEN) + 1)),
         OPP_PRI_CAMP_SRC_SCORE = floor(rank(OPP_PRI_CAMP_SRC_LEN)*100/(length(OPP_PRI_CAMP_SRC_LEN) + 1)),
         OPP_HPSOLUTION_SCORE   = floor(rank(OPP_HPSOLUTION_LEN)*100/(length(OPP_HPSOLUTION_LEN) + 1)),
         OPP_PRI_PARTN_SCORE    = floor(rank(OPP_PRI_PARTN_LEN)*100/(length(OPP_PRI_PARTN_LEN) + 1)),
         OPP_PRI_COMP_SCORE     = floor(rank(OPP_PRI_COMP_LEN)*100/(length(OPP_PRI_COMP_LEN) + 1))
  )


# Clean Work Space
rm(opp_final)
rm(opp_temp)
rm(stra_temp)

# Save processed archive data to RDS data
saveRDS(abp, "abp.RDS")
saveRDS(acct, "acct.RDS")
saveRDS(acctcustomercontact, "acctcustomercontact.RDS")
saveRDS(bugsoppty, "bugsoppty.RDS")
saveRDS(bup, "bup.RDS")
saveRDS(bus, "bus.RDS")
saveRDS(compls, "compls.RDS")
saveRDS(custprior, "custprior.RDS")
saveRDS(opp, "opp.RDS")
saveRDS(partnerlandscape, "partnerlandscape.RDS")
saveRDS(scorecard, "scorecard.RDS")
saveRDS(stratinit, "stratinit.RDS")
saveRDS(tce, "tce.RDS")


# abp                 <- readRDS("abp.RDS")
# acct                <- readRDS("acct.RDS")
# acctcustomercontact <- readRDS("acctcustomercontact.RDS")
# bugsoppty           <- readRDS("bugsoppty.RDS")
# bup                 <- readRDS("bup.RDS")
# bus                 <- readRDS("bus.RDS")
# compls              <- readRDS("compls.RDS")
# custprior           <- readRDS("custprior.RDS")
# opp                 <- readRDS("opp.RDS")
# partnerlandscape    <- readRDS("partnerlandscape.RDS")
# scorecard           <- readRDS("scorecard.RDS")
# stratinit           <- readRDS("stratinit.RDS")
# tce                 <- readRDS("tce.RDS")

################################################################################
##################### Step 5 Export the Data as Flat Files #####################
################################################################################

# Find the End of Month Date
End_Month <- function(date) {
  # date character string containing POSIXct date
  date.lt <- as.POSIXlt(date) # add a month, then subtract a day:
  mon <- date.lt$mon + 2 
  year <- date.lt$year
  year <- year + as.integer(mon==13) # if month was December add a year
  mon[mon==13] <- 1
  
  result = as.Date(ISOdate(1900+year, mon, 1, hour=0, tz="America/Chicago")) - 1
  result
}

# Replace NA to "Not Specified" for character variables; replace NA to blank for numeric variables to match current QV model
# Remove Line Break to avoid problem when exporting to TXT
Replace_NA_LB <- function(mydata) {
  
  char_cols <- colnames(mydata)[sapply(mydata, class) == "character"]
  
  num_cols <- colnames(mydata)[sapply(mydata, class) == "numeric"]
  
  date_cols <- c("ABP_CREATED_DT", "ABP_LAST_ACTIVITY_DT", "ABP_LAST_CUST_ENGGMNT", "ABP_LAST_MOD_DT", "ABP_NEXT_CUST_ENGGMNT", 
                 "ACCT_CREATED_DT", "ACCT_LAST_ACTIVITY", "ACCT_LAST_COVRG_DT", "ACCT_LAST_IND_DT", "ACCT_LAST_MOD_DT",
                 "ACCT_LAST_PROF_DT", "ACCT_LASTGEOASSIGN_DT", "ACCT_MDCP_LAST_SYNC", "ACCT_RPL_LAST_SYNC", "BUP_CREATED_DT",
                 "BUP_LAST_ACTIVITY_DT", "BUP_LAST_MOD_DT", "BUS_CREATED_DT", "BUS_LAST_ACT_DT", "BUS_LAST_MOD_DT", "CBP_CREATED_DT",
                 "CL_LAST_MOD_DT", "CREATED_DT", "CRM_CREATED_DT", "CRM_LAST_MOD_DT", "OPP_CLOSE_DT", "OPP_CREATED_DT", "OPP_LAST_MOD_DT",
                 "PARTN_CREATED_DT", "SC_CREATED_DT", "SC_LAST_ACT_DT", "SC_LAST_MOD_DT", "SI_CREATED_DT", "SI_LAST_ACTIVITY_DT",
                 "TCE_CREATED_DT", "TCE_LAST_ACT_DT")
  
  
  for (i in char_cols) {
    
    mydata[[i]] <- iconv(mydata[[i]], to="ASCII", sub="")
    
    if (!i %in% date_cols) {
      
      set(mydata, which(nchar(iconv(mydata[[i]], to="ASCII", sub = ""))==0 | is.na(iconv(mydata[[i]], to="ASCII", sub = ""))), i, "Not Specified")
      
      mydata[[i]] <- gsub("[\r\n]", "", mydata[[i]])
    }}
  
  
  # for (i in num_cols) {
  #   
  #   set(mydata, which(is.na(mydata[[i]])), i, numeric(0))
  #   }
  
  mydata
}

# Replace | as it will be used as delimiter 
# Replace arrow sign (->) as it will cause problem when uploading to Qlikview
Replace_Slash <- function(mydata) {
  
  cols <- colnames(mydata)[sapply(mydata, class) == "character"]
  
  for (i in cols) {
    mydata[[i]] <- gsub(pattern="\\|", replacement = "/", mydata[[i]])
    
    mydata[[i]] <- gsub(pattern="\032", replacement = "", mydata[[i]])
  }
  mydata
}

# Add column placeholder to match previous data structure
Add_Columns <- function(mydata) {
  
  cols <- colnames(mydata)
  new_cols <- c("BUP_DETL", "BUP_DETL_STAT", "BUP_ISSUES", "BUP_KEY_CONTACT", "BUP_NOTES", "BUP_PPT_ITG_DESC", 
                "ABP_CUST_STRAT", "ABP_EXEC_ASKS", "ABP_HP_CUST_STATE", "ABP_KEY_ISSUE", "ABP_STRAT_OPP",
                "OPP_HPSOLUTION", "OPP_NAME", "OPP_NM", "BUP_PLAN_NM", "CRM_TITLE", "CRM_ADDTNL_INFO", "CRM_BUCONTACT",
                "OPP_PRI_CHNLPARTN_ACCT", "OPP_PRI_CAMP_NM", "OPP_PRI_CAMP_SOURCE", 
                "OPP_PRI_COMP", "OPP_OWNER", "OPP_OWNER_NM", "CREATOR", "OPP_LAST_MOD_NM")
  
  new_cols <- new_cols[! new_cols %in% cols]
  
  mydata[new_cols] <- " "
  
  mydata
}


Process_Export <- function(mydata) {
  
  data_name <- deparse(substitute(mydata))
  
  # Shrink some long columns to limit file size
  shrink_cols <- 
    unique(c("BUP_DETL", "BUP_DETL_STAT", "BUP_ISSUES", "BUP_KEY_CONTACT", "BUP_NOTES", "BUP_PPT_ITG_DESC", #bup
             "ABP_CUST_STRAT", "ABP_EXEC_ASKS", "ABP_HP_CUST_STATE", "ABP_KEY_ISSUE", "ABP_STRAT_OPP",      #abp
             "OPP_HPSOLUTION", "OPP_NAME", "OPP_NM", "BUP_Plan_NM",                                         #bugspooty
             "CRM_TITLE", "CRM_ADDTNL_INFO", "CRM_BUCONTACT",                                               #acctcustomercontact
             "OPP_PRI_CHNLPARTN_ACCT", "OPP_PRI_CAMP_NM", "OPP_PRI_CAMP_SOURCE", "OPP_PRI_COMP",            
             "OPP_OWNER", "OPP_OWNER_NM",                                                                   #opp
             "BUP_DETL", "BUP_DETL_STAT", "CREATOR", "OPP_LAST_MOD_NM"                                      #bup
    ))
  
  cols <- colnames(mydata)[sapply(mydata, class) == "character"]
  
  if (data_name %in% c("bup", "abp", "bugspooty", "acctcustomercontact", "opp", "bup")) {
    
    for (i in cols) {
      
      if (i %in% shrink_cols) {
        
        set(mydata, which(mydata[["SNAP_DT"]] %in% c("31JUL2015", "27AUG2015", "24SEP2015", "20OCT2015", "25NOV2015", "10DEC2015", 
                                                     "28JAN2016", "25FEB2016", "31MAR2016", "28APR2016", "26MAY2016", "30JUN2016", 
                                                     "21JUL2016", "29JUL2016", "25AUG2016", "29SEP2016", "28OCT2016", "22NOV2016",
                                                     "21DEC2016", "19JAN2017", "02FEB2017", "09FEB2017", "16FEB2017")), i, " ")
      }}}
  
  
  # Identify sensitive information by report name
  ifelse(data_name=="abp", fields <- c("ACCT_PLAN_NM", "ABP_CUST_DFN_INNVTN", "ABP_HP_INNVTN_STRAT", 
                                       "ABP_STRAT_OPP", "ABP_KEY_ISSUE", "ABP_CUST_STRAT", "ABP_HP_CUST_STATE"), 
         
         ifelse(data_name=="acct", fields <- c("ACCT_NM", "ACCT_LATIN_NM", "ACCT_ALT_NM"),
                
         ifelse(data_name=="acctcustomercontact", fields <- c("ACCT_PLAN_NM"),
                       
         ifelse(data_name=="bugsoppty", fields <- c("ACCT_PLAN_NM", "BUP_PLAN_NM", "BUS_NM", "OPP_NM", "OPP_NAME"),
                              
         ifelse(data_name=="bup", fields <- c("ACCT_PLAN_NM", "BUP_DETL_STAT", "BUP_KEY_CONTACT", "BUP_ISSUES", "BUP_DETL"),
                                     
         ifelse(data_name=="bus", fields <- c("ACCT_PLAN_NM", "BUS_NM", "BUS_EXECTN_ACT", "BUS_COMP_REACT", 
                                              "BUS_PARTN_INFLNCE_STRAT", "BUS_REQ_PROC_CHANGE"),
                                            
         ifelse(data_name=="compls", fields <- c("ACCT_PLAN_NM", "CL_COMMENTS"),
                                                   
         ifelse(data_name=="custprior", fields <- c("ACCT_PLAN_NM", "CBP_NM", "CBP_EXP_OUTCOME", "CBP_DESC", "CBP_DETL_DESC"),       
                                                          
         ifelse(data_name=="opp", fields <- c("ACCT_PLAN_NM", "OPP_NM"),   
                                                                 
         ifelse(data_name=="partnerlandscape", fields <- c("ACCT_PLAN_NM", "PARTN_HP_STRAT", "PARTN_FOCUS"),       
                                                                        
         ifelse(data_name=="scorecard", fields <- c("ACCT_PLAN_NM"),
                                                                               
         ifelse(data_name=="stratinit", fields <- c("ACCT_PLAN_NM", "SI_NM", "SI_DESC", "SI_CUST_BUS_CASE", 
                                                    "SI_RISK", "SI_DEPENDENCIES", "SI_CUST_SPONSOR"),       
                                                                                      
         ifelse(data_name=="tce", fields<-c("ACCT_PLAN_NM"), print(data_name) & stop("File name is not recognized!")
                )))))))))))))
  
  
  # Mask sensitive information for private clients
  acctids <- acct %>% 
    filter(ACCT_PRIV=="1") %>%
    select(ACCT_PLAN_ID) %>%
    unique()
  
  acctids <- acctids[["ACCT_PLAN_ID"]]
  
  
  for (i in fields) {
    set(mydata, which(mydata[["ACCT_PLAN_ID"]] %in% acctids), i, "**********")
  }
  
  
  # Filter data based on business rules
  mydata1 <- mydata %>%
    
    filter(!SNAP_DT %in% c("27SEP2013", "25OCT2013", "29NOV2013", "27DEC2013", "30JAN2014", "27FEB2014", "27MAR2014", 
                           "24APR2014", "29MAY2014", "27NOV2014", "18DEC2014", "29JAN2015", "26FEB2015", "27MAR2015", 
                           "30APR2015", "29MAY2015", "25JUN2015", "31JUL2015", "27AUG2015", "24SEP2015", "29OCT2015",
                           "25NOV2015", "10DEC2015", "28JAN2016", "25FEB2016", "20JUL2017")) %>%
    
    # Filter only historic month end snap_dt or last three weeks snap_dt
    # 10DEC2015 is included because there is no update occured during month end of DEC2015
    filter((as.Date(SNAP_DT,"%d%b%Y") >= End_Month(as.Date(SNAP_DT,"%d%b%Y"))-6 & 
              as.Date(SNAP_DT,"%d%b%Y") <= End_Month(as.Date(SNAP_DT,"%d%b%Y"))) | 
             as.Date(SNAP_DT,"%d%b%Y") + 23 > as.Date(current_day,"%d%b%Y") |
             SNAP_DT %in% c("10DEC2015") |
             SNAP_DT %in% c("22NOV2016") |
             SNAP_DT %in% c("21DEC2016")) %>%
    arrange(as.Date(SNAP_DT, format = '%d%b%Y'))
  
  
  # Call functions to replace NA and Slash
  mydata1 <- Replace_NA_LB(mydata1)
  mydata1 <- Replace_Slash(mydata1)
  
  # Call functions to 
  mydata1 <- Add_Columns(mydata1)
  
  
  # Change column lengths to match ART Platform
  ifelse(data_name=="abp", mydata1 <- mydata1 %>% 
           mutate(ACCT_ID = substr(ACCT_ID, 1, 15),
                  ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                  ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 81),
                  ABP_ANNUAL_SLS_CURRENCY = substr(ABP_ANNUAL_SLS_CURRENCY, 1, 3),
                  ABP_ANNUAL_SLS_CONV = substr(ABP_ANNUAL_SLS_CONV, 1, 3),
                  ABP_APPROVER = substr(ABP_APPROVER, 1, 32),
                  ABP_CUST_DFNTN_AGRD = substr(ABP_CUST_DFNTN_AGRD, 1, 1),
                  ABP_CUST_INDSTRY = substr(ABP_CUST_INDSTRY, 1, 40),
                  ABP_CUST_PPTITG_DESCR = substr(ABP_CUST_PPTITG_DESCR, 1, 158),
                  ABP_CUST_STRAT = substr(ABP_CUST_STRAT, 1, 500),
                  ABP_EXEC_ASKS = substr(ABP_EXEC_ASKS, 1, 50),
                  ABP_EXEC_SPONSOR = substr(ABP_EXEC_SPONSOR, 1, 32),
                  ABP_FISCAL_YEAREND = substr(ABP_FISCAL_YEAREND, 1, 9),
                  ABP_IA_GUIDANCE = substr(ABP_IA_GUIDANCE, 1, 281),
                  ABP_Y11_REV_CURRENCY = substr(ABP_Y11_REV_CURRENCY, 1, 3),
                  ABP_Y11_REV_CONV = substr(ABP_Y11_REV_CONV, 1, 3),
                  ABP_Y12_REV_CURRENCY = substr(ABP_Y12_REV_CURRENCY, 1, 3),
                  ABP_Y12_REV_CONV = substr(ABP_Y12_REV_CONV, 1, 3),
                  ABP_Y13_REV_CURRENCY = substr(ABP_Y13_REV_CURRENCY, 1, 3),
                  ABP_Y13_REV_CONV = substr(ABP_Y13_REV_CONV, 1, 3),
                  ABP_Y14_REV_CURRENCY = substr(ABP_Y14_REV_CURRENCY, 1, 3),
                  ABP_Y14_REV_CONV = substr(ABP_Y14_REV_CONV, 1, 3),
                  ABP_Y15_REV_CURRENCY = substr(ABP_Y15_REV_CURRENCY, 1, 3),
                  ABP_Y15_REV_CONV = substr(ABP_Y15_REV_CONV, 1, 3),
                  ABP_GOVN = substr(ABP_GOVN, 1, 21),
                  ABP_CUST_DFN_INNVTN = substr(ABP_CUST_DFN_INNVTN, 1, 993),
                  ABP_HP_CUST_GOVN = substr(ABP_HP_CUST_GOVN, 1, 983),
                  ABP_GOTOMARKET_SEG = substr(ABP_GOTOMARKET_SEG, 1, 28),
                  ABP_HP_INNVTN_STRAT = substr(ABP_HP_INNVTN_STRAT, 1, 998),
                  ABP_STRAT_OPP = substr(ABP_STRAT_OPP, 1, 3171),
                  ABP_HQ_CNTRY = substr(ABP_HQ_CNTRY, 1, 20),
                  ABP_HQ_REG = substr(ABP_HQ_REG, 1, 4),
                  ABP_HQ_SUBREG = substr(ABP_HQ_SUBREG, 1, 15),
                  ABP_INACTIVE_PLAN = substr(ABP_INACTIVE_PLAN, 1, 1),
                  ABP_IND_SEG = substr(ABP_IND_SEG, 1, 37),
                  ABP_INNVTN_ENGGMNT_REF = substr(ABP_INNVTN_ENGGMNT_REF, 1, 3),
                  ABP_KEY_ISSUE = substr(ABP_KEY_ISSUE, 1, 2458),
                  ABP_LEGAL_NM = substr(ABP_LEGAL_NM, 1, 80),
                  ABP_NOTE = substr(ABP_NOTE, 1, 44),
                  ABP_NUM_EMPLOYEE = substr(ABP_NUM_EMPLOYEE, 1, 20),
                  ABP_OWNER_EMAIL = substr(ABP_OWNER_EMAIL, 1, 1),
                  ABP_PLAN_STATUS = substr(ABP_PLAN_STATUS, 1, 26),
                  ABP_PRIV_ACCT = substr(ABP_PRIV_ACCT, 1, 1),
                  ABP_HP_CUST_STATE = substr(ABP_HP_CUST_STATE, 1, 3058),
                  ABP_PPTITG_DESCR = substr(ABP_PPTITG_DESCR, 1, 50),
                  ABP_TCE_PRIOR = substr(ABP_TCE_PRIOR, 1, 1647),
                  ABP_URL1 = substr(ABP_URL1, 1, 224),
                  ABP_URL2 = substr(ABP_URL2, 1, 224),
                  ABP_URL3 = substr(ABP_URL1, 1, 236),
                  ABP_CURRENCY = substr(ABP_CURRENCY, 1, 3),
                  ABP_OWNER_NM = substr(ABP_OWNER_NM, 1, 26),
                  ABP_OWNER_ALIAS = substr(ABP_OWNER_ALIAS, 1, 8),
                  ABP_OWNER_ROLE = substr(ABP_OWNER_ROLE, 1, 63),
                  ABP_CREATOR = substr(ABP_CREATOR, 1, 20),
                  ABP_CREATED_ALIAS = substr(ABP_CREATED_ALIAS, 1, 6),
                  ABP_LAST_MOD_NM = substr(ABP_LAST_MOD_NM, 1, 26),
                  ABP_LAST_MOD_ALIAS = substr(ABP_LAST_MOD_ALIAS, 1, 8)), 
         
         ifelse(data_name=="acct", mydata1 <- mydata1 %>% 
                  mutate(SNAP_DT,
                         ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                         ACCT_ID = substr(ACCT_ID, 1, 15),
                         ACCT_NM = substr(ACCT_NM, 1, 95),
                         ACCT_OWNER = substr(ACCT_OWNER, 1, 24),
                         ACCT_OWNER_ALIAS = substr(ACCT_OWNER_ALIAS, 1, 8),
                         ACCT_CURRENCY = substr(ACCT_CURRENCY, 1, 3),
                         ACCT_ANNUAL_REV_CURRENCY = substr(ACCT_ANNUAL_REV_CURRENCY, 1, 3),
                         ACCT_ANNUAL_REV_CONV = substr(ACCT_ANNUAL_REV_CONV, 1, 3),
                         ACCT_REC_TYPE = substr(ACCT_REC_TYPE, 1, 8),
                         ACCT_CREATOR = substr(ACCT_CREATOR, 1, 14),
                         ACCT_CREATED_ALIAS = substr(ACCT_CREATED_ALIAS, 1, 5),
                         ACCT_LAST_MOD_NM = substr(ACCT_LAST_MOD_NM, 1, 25),
                         ACCT_LAST_MOD_ALIAS = substr(ACCT_LAST_MOD_ALIAS, 1, 8),
                         ACCT_AMID = substr(ACCT_AMID, 1, 11),
                         ACCT_LATIN_NM = substr(ACCT_LATIN_NM, 1, 95),
                         ACCT_ALLIANCE_FLAG = substr(ACCT_ALLIANCE_FLAG, 1, 1),
                         ACCT_ALLIANCE_ROLE = substr(ACCT_ALLIANCE_ROLE, 1, 1),
                         ACCT_CHNNL_FLAG = substr(ACCT_CHNNL_FLAG, 1, 1),
                         ACCT_PRIV = substr(ACCT_PRIV, 1, 1),
                         ACCT_DUN = substr(ACCT_DUN, 1, 9),
                         ACCT_UDUN = substr(ACCT_UDUN, 1, 9),
                         ACCT_GDUN = substr(ACCT_GDUN, 1, 9),
                         ACCT_LOC_ID = substr(ACCT_LOC_ID, 1, 1),
                         ACCT_MDCP = substr(ACCT_MDCP, 1, 9),
                         ACCT_PARENT_PARTNER_ID = substr(ACCT_PARENT_PARTNER_ID, 1, 1),
                         ACCT_PARENT_PARTNER_NM = substr(ACCT_PARENT_PARTNER_NM, 1, 1),
                         ACCT_PARTNER_STATUS = substr(ACCT_PARTNER_STATUS, 1, 5),
                         ACCT_PARTNER_TYPE = substr(ACCT_PARTNER_TYPE, 1, 1),
                         ACCT_VALID_REGION = substr(ACCT_VALID_REGION, 1, 13),
                         ACCT_SOR_ACCT_ID = substr(ACCT_SOR_ACCT_ID, 1, 20),
                         ACCT_ALT_NM = substr(ACCT_ALT_NM, 1, 50),
                         ACCT_SEG = substr(ACCT_SEG, 1, 151),
                         ACCT_IND_SEG = substr(ACCT_IND_SEG, 1, 30),
                         ACCT_IND_VERT = substr(ACCT_IND_VERT, 1, 27),
                         ACCT_NAMEDACCT = substr(ACCT_NAMEDACCT, 1, 29),
                         ACCT_STANDARD_ACCT = substr(ACCT_STANDARD_ACCT, 1, 1),
                         ACCT_PARTNERPORT_EGLIG = substr(ACCT_PARTNERPORT_EGLIG, 1, 1),
                         ACCT_RAD = substr(ACCT_RAD, 1, 118),
                         ACCT_TAXID = substr(ACCT_TAXID, 1, 20),
                         ACCT_NAME2 = substr(ACCT_NAME2, 1, 97),
                         ACCT_NAME3 = substr(ACCT_NAME3, 1, 114),
                         ACCT_NAME4 = substr(ACCT_NAME4, 1, 90),
                         ACCT_NAME5 = substr(ACCT_NAME5, 1, 92),
                         ACCT_GEO_HIERARCHY = substr(ACCT_GEO_HIERARCHY, 1, 60),
                         ACCT_PROFILE = substr(ACCT_PROFILE, 1, 1),
                         ACCT_CHATTER_BL_TYPE = substr(ACCT_CHATTER_BL_TYPE, 1, 1),
                         ACCT_COUNTRY_CD = substr(ACCT_COUNTRY_CD, 1, 2),
                         ACCT_HP_LEAD_STAT = substr(ACCT_HP_LEAD_STAT, 1, 1),
                         ACCT_HP_SFDC_ACCESS = substr(ACCT_HP_SFDC_ACCESS, 1, 1),
                         ACCT_MDCP_SUB = substr(ACCT_MDCP_SUB, 1, 4),
                         ACCT_GEO_HIERARCHY2 = substr(ACCT_GEO_HIERARCHY2, 1, 60),
                         ACCT_REG = substr(ACCT_REG, 1, 12),
                         ACCT_SUBREG1 = substr(ACCT_SUBREG1, 1, 16),
                         ACCT_SUBREG2 = substr(ACCT_SUBREG2, 1, 22),
                         ACCT_SUBREG3 = substr(ACCT_SUBREG3, 1, 18),
                         ACCT_WORLD_REG = substr(ACCT_WORLD_REG, 1, 2),
                         ACCT_BG_TARGET_SEG = substr(ACCT_BG_TARGET_SEG, 1, 61),
                         ACCT_CASE_SAFE_ID = substr(ACCT_CASE_SAFE_ID, 1, 18),
                         ACCT_BUS_RELTNSHP = substr(ACCT_BUS_RELTNSHP, 1, 1),
                         ACCT_MDCP_ORG_ID = substr(ACCT_MDCP_ORG_ID, 1, 1),
                         ACCT_MDCP_SITE_ID = substr(ACCT_MDCP_SITE_ID, 1, 9),
                         ACCT_MDCP_SUB_FLAG = substr(ACCT_MDCP_SUB_FLAG, 1, 4),
                         ACCT_DB_OUTOFBUS_FLAG = substr(ACCT_DB_OUTOFBUS_FLAG, 1, 5),
                         ACCT_FTP_INFO = substr(ACCT_FTP_INFO, 1, 1),
                         ACCT_MDCP_BUS_ID = substr(ACCT_MDCP_BUS_ID, 1, 1),
                         ACCT_ORG_DUN = substr(ACCT_ORG_DUN, 1, 9),
                         ACCT_ORG_UNIT = substr(ACCT_ORG_UNIT, 1, 9),
                         ACCT_PRIV_BUS_RLTNSHP = substr(ACCT_PRIV_BUS_RLTNSHP, 1, 1),
                         ACCT_RPL_STAT = substr(ACCT_RPL_STAT, 1, 14),
                         ACCT_REC_SUBTYPE = substr(ACCT_REC_SUBTYPE, 1, 1),
                         ACCT_SUPPORT_TYPE = substr(ACCT_SUPPORT_TYPE, 1, 1),
                         ACCT_USERKEY = substr(ACCT_USERKEY, 1, 12)),
                
         ifelse(data_name=="acctcustomercontact", mydata1 <- mydata1 %>% 
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 65),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 81),
                         CRM_ID = substr(CRM_ID, 1, 15),
                         CRM_NM = substr(CRM_NM, 1, 9),
                         CRM_ADDTNL_INFO = substr(CRM_ADDTNL_INFO, 1, 279),
                         CRM_HPCONTACT = substr(CRM_HPCONTACT, 1, 27),
                         CRM_BGCONTACT = substr(CRM_BGCONTACT, 1, 9),
                         CRM_BUCONTACT = substr(CRM_BUCONTACT, 1, 91),
                         CRM_PP_S1 = substr(CRM_PP_S1, 1, 21),
                         CRM_PP_S10 = substr(CRM_PP_S10, 1, 21),
                         CRM_PP_S11 = substr(CRM_PP_S11, 1, 21),
                         CRM_PP_S12 = substr(CRM_PP_S12, 1, 21),
                         CRM_PP_S2 = substr(CRM_PP_S2, 1, 21),
                         CRM_PP_S3 = substr(CRM_PP_S3, 1, 21),
                         CRM_PP_S4 = substr(CRM_PP_S4, 1, 21),
                         CRM_PP_S5 = substr(CRM_PP_S5, 1, 21),
                         CRM_PP_S6 = substr(CRM_PP_S6, 1, 21),
                         CRM_PP_S7 = substr(CRM_PP_S7, 1, 21),
                         CRM_PP_S8 = substr(CRM_PP_S8, 1, 21),
                         CRM_PP_S9 = substr(CRM_PP_S9, 1, 21),
                         CRM_RELATIONSHIPTYPE = substr(CRM_RELATIONSHIPTYPE, 1, 19),
                         CRM_REPORTSTO = substr(CRM_REPORTSTO, 1, 39),
                         CRM_RM_S1 = substr(CRM_RM_S1, 1, 21),
                         CRM_RM_S10 = substr(CRM_RM_S10, 1, 21),
                         CRM_RM_S2 = substr(CRM_RM_S2, 1, 21),
                         CRM_RM_S3 = substr(CRM_RM_S3, 1, 21),
                         CRM_RM_S4 = substr(CRM_RM_S4, 1, 21),
                         CRM_RM_S5 = substr(CRM_RM_S5, 1, 21),
                         CRM_RM_S6 = substr(CRM_RM_S6, 1, 21),
                         CRM_RM_S7 = substr(CRM_RM_S7, 1, 21),
                         CRM_RM_S8 = substr(CRM_RM_S8, 1, 21),
                         CRM_RM_S9 = substr(CRM_RM_S9, 1, 21),
                         CRM_ROLE = substr(CRM_ROLE, 1, 21),
                         CRM_TITLE = substr(CRM_TITLE, 1, 314),
                         CRM_CURRENCY = substr(CRM_CURRENCY, 1, 3),
                         CRM_CREATOR = substr(CRM_CREATOR, 1, 28),
                         CRM_CREATED_ALIAS = substr(CRM_CREATED_ALIAS, 1, 8),
                         CRM_LAST_MOD_NM = substr(CRM_LAST_MOD_NM, 1, 28),
                         CRM_LAST_MOD_ALIAS = substr(CRM_LAST_MOD_ALIAS, 1, 8),
                         CRM_LAST_ACT_DT = substr(CRM_LAST_ACT_DT, 1, 1),
                         CONTACT_FIRSTNAME = substr(CONTACT_FIRSTNAME, 1, 22),
                         CONTACT_LASTNAME = substr(CONTACT_LASTNAME, 1, 34),
                         CONTACT_CONTACTID = substr(CONTACT_CONTACTID, 1, 15)),
                       
         ifelse(data_name=="bugsoppty", mydata1 <- mydata1 %>% 
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 65),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 81),
                         BUP_ID = substr(BUP_ID, 1, 15),
                         BUP_PLAN_NM = substr(BUP_PLAN_NM, 1, 72),
                         BUP_BG_NM = substr(BUP_BG_NM, 1, 4),
                         BUS_ID = substr(BUS_ID, 1, 15),
                         BUS_NM = substr(BUS_NM, 1, 80),
                         BUS_TYPE = substr(BUS_TYPE, 1, 124),
                         CREATOR = substr(CREATOR, 1, 28),
                         OPP_CURRENCY = substr(OPP_CURRENCY, 1, 3),
                         OPP_LAST_MOD_NM = substr(OPP_LAST_MOD_NM, 1, 28),
                         OPP_NM = substr(OPP_NM, 1, 124),
                         OPP_RBUO_ID = substr(OPP_RBUO_ID, 1, 15),
                         OPP_RBUO_NM = substr(OPP_RBUO_NM, 1, 9),
                         OPP_RBUO_KEY = substr(OPP_RBUO_KEY, 1, 33),
                         OPP_CREATOR = substr(OPP_CREATOR, 1, 47),
                         OPP_ID = substr(OPP_ID, 1, 14),
                         OPP_HPSOLUTION = substr(OPP_HPSOLUTION, 1, 28),
                         OPP_NAME = substr(OPP_NAME, 1, 124),
                         OPP_SLS_STAGE = substr(OPP_SLS_STAGE, 1, 28),
                         OPP_FORECAST_CAT = substr(OPP_FORECAST_CAT, 1, 8),
                         OPP_GOTOMARKET_ROUTE = substr(OPP_GOTOMARKET_ROUTE, 1, 8),
                         OPP_TTL_VAL_CURRENCY = substr(OPP_TTL_VAL_CURRENCY, 1, 3),
                         OPP_TTL_CON_CURRENCY = substr(OPP_TTL_CON_CURRENCY, 1, 3)),
                              
         ifelse(data_name=="bup", mydata1 <- mydata1 %>% 
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 80),
                         BUP_ID = substr(BUP_ID, 1, 15),
                         BUP_PLAN_NM = substr(BUP_PLAN_NM, 1, 72),
                         BUP_DETL = substr(BUP_DETL, 1, 50),
                         BUP_BG_NM = substr(BUP_BG_NM, 1, 4),
                         BUP_EXEC_SPONSOR = substr(BUP_EXEC_SPONSOR, 1, 24),
                         BUP_NM = substr(BUP_NM, 1, 41),
                         BUP_OWNER = substr(BUP_OWNER, 1, 28),
                         BUP_PPT_ITG_DESC = substr(BUP_PPT_ITG_DESC, 1, 576),
                         BUP_STATUS = substr(BUP_STATUS, 1, 26),
                         BUP_COUNTRY = substr(BUP_COUNTRY, 1, 20),
                         BUP_DETL_STAT = substr(BUP_DETL_STAT, 1, 50),
                         BUP_TAM_CURRENCY = substr(BUP_TAM_CURRENCY, 1, 3),
                         BUP_TAM_CONV = substr(BUP_TAM_CONV, 1, 3),
                         BUP_FY11_SLS_CURRENCY = substr(BUP_FY11_SLS_CURRENCY, 1, 3),
                         BUP_FY11_SLS_CONV = substr(BUP_FY11_SLS_CONV, 1, 3),
                         BUP_FY12_SLS_CURRENCY = substr(BUP_FY12_SLS_CURRENCY, 1, 3),
                         BUP_FY12_SLS_CONV = substr(BUP_FY12_SLS_CONV, 1, 3),
                         BUP_FY13_P_SLS_CURRENCY = substr(BUP_FY13_P_SLS_CURRENCY, 1, 3),
                         BUP_FY13_P_SLS_CONV = substr(BUP_FY13_P_SLS_CONV, 1, 3),
                         BUP_FY13_A_SLS_CURRENCY = substr(BUP_FY13_A_SLS_CURRENCY, 1, 3),
                         BUP_FY13_A_SLS_CONV = substr(BUP_FY13_A_SLS_CONV, 1, 3),
                         BUP_FY14_P_SLS_CURRENCY = substr(BUP_FY14_P_SLS_CURRENCY, 1, 3),
                         BUP_FY14_P_SLS_CONV = substr(BUP_FY14_P_SLS_CONV, 1, 3),
                         BUP_FY14_A_SLS_CURRENCY = substr(BUP_FY14_A_SLS_CURRENCY, 1, 3),
                         BUP_FY14_A_SLS_CONV = substr(BUP_FY14_A_SLS_CONV, 1, 3),
                         BUP_FY15_SLS_CURRENCY = substr(BUP_FY15_SLS_CURRENCY, 1, 3),
                         BUP_FY15_SLS_CONV = substr(BUP_FY15_SLS_CONV, 1, 3),
                         BUP_FY16_SLS_CURRENCY = substr(BUP_FY16_SLS_CURRENCY, 1, 3),
                         BUP_FY16_SLS_CONV = substr(BUP_FY16_SLS_CONV, 1, 3),
                         BUP_FY17_SLS_CURRENCY = substr(BUP_FY17_SLS_CURRENCY, 1, 3),
                         BUP_FY17_SLS_CONV = substr(BUP_FY17_SLS_CONV, 1, 3),
                         BUP_KEY_CONTACT = substr(BUP_KEY_CONTACT, 1, 1051),
                         BUP_NOTES = substr(BUP_NOTES, 1, 3747),
                         BUP_ISSUES = substr(BUP_ISSUES, 1, 3249),
                         BUP_REGION = substr(BUP_REGION, 1, 4),
                         BUP_SUBREGION = substr(BUP_SUBREGION, 1, 15),
                         BUP_YOY1 = substr(BUP_YOY1, 1, 21),
                         BUP_YOY2 = substr(BUP_YOY2, 1, 20),
                         BUP_YOY3 = substr(BUP_YOY3, 1, 20),
                         BUP_YOY4 = substr(BUP_YOY4, 1, 21),
                         BUP_YOY5 = substr(BUP_YOY5, 1, 19),
                         BUP_CURRENCY = substr(BUP_CURRENCY, 1, 3),
                         BUP_CREATOR = substr(BUP_CREATOR, 1, 29),
                         BUP_CREATOR_ALIAS = substr(BUP_CREATOR_ALIAS, 1, 8),
                         BUP_LAST_MOD_NM = substr(BUP_LAST_MOD_NM, 1, 29),
                         BUP_LAST_MOD_ALIAS = substr(BUP_LAST_MOD_ALIAS, 1, 8)) %>% 
                  
                  select(SNAP_DT, ACCT_PLAN_ID, ACCT_PLAN_NM, BUP_ID, BUP_PLAN_NM, BUP_DETL, BUP_BG_NM, BUP_EXEC_SPONSOR, 
                         BUP_NM, BUP_AVG_SCORE, BUP_OWNER, BUP_PPT_ITG_DESC, BUP_STATUS, BUP_COUNTRY, BUP_DETL_STAT, 
                         BUP_SOW, BUP_TAM_CURRENCY, BUP_TAM_AMT, BUP_TAM_CONV, BUP_TAM_AMT_CONV, BUP_FY11_SLS_CURRENCY, 
                         BUP_FY11_SLS_AMT, BUP_FY11_SLS_CONV, BUP_FY11_SLS_AMT_CONV, BUP_FY12_SLS_CURRENCY, BUP_FY12_SLS_AMT, 
                         BUP_FY12_SLS_CONV, BUP_FY12_SLS_AMT_CONV, BUP_FY13_P_SLS_CURRENCY, BUP_FY13_P_SLS_AMT, 
                         BUP_FY13_P_SLS_CONV, BUP_FY13_P_SLS_AMT_CONV, BUP_FY13_A_SLS_CURRENCY, BUP_FY13_A_SLS_AMT, 
                         BUP_FY13_A_SLS_CONV, BUP_FY13_A_SLS_AMT_CONV, BUP_FY14_P_SLS_CURRENCY, BUP_FY14_P_SLS_AMT, 
                         BUP_FY14_P_SLS_CONV, BUP_FY14_P_SLS_AMT_CONV, BUP_FY14_A_SLS_CURRENCY, BUP_FY14_A_SLS_AMT, 
                         BUP_FY14_A_SLS_CONV, BUP_FY14_A_SLS_AMT_CONV, BUP_FY15_SLS_CURRENCY, BUP_FY15_SLS_AMT, 
                         BUP_FY15_SLS_CONV, BUP_FY15_SLS_AMT_CONV, BUP_FY16_SLS_CURRENCY, BUP_FY16_SLS_AMT, BUP_FY16_SLS_CONV, 
                         BUP_FY16_SLS_AMT_CONV, BUP_FY17_SLS_CURRENCY, BUP_FY17_SLS_AMT, BUP_FY17_SLS_CONV, BUP_FY17_SLS_AMT_CONV, 
                         BUP_FY18_SLS_CURRENCY, BUP_FY18_SLS_AMT, BUP_FY18_SLS_CONV, BUP_FY18_SLS_AMT_CONV, BUP_KEY_CONTACT, 
                         BUP_NOTES, BUP_ISSUES, BUP_Q1, BUP_Q2, BUP_Q3, BUP_Q4, BUP_Q5, BUP_REGION, BUP_SUBREGION, BUP_YOY1, 
                         BUP_YOY2, BUP_YOY3, BUP_YOY4, BUP_YOY5, BUP_CURRENCY, BUP_CREATOR, BUP_CREATOR_ALIAS, BUP_CREATED_DT, 
                         BUP_LAST_MOD_NM, BUP_LAST_MOD_ALIAS, BUP_LAST_MOD_DT, BUP_LAST_ACTIVITY_DT, BUP_NOTES_LEN, BUP_DETL_LEN, 
                         BUP_ISSUES_LEN, BUP_DETL_STAT_LEN, BUP_SOW_SCORE, BUP_TAM_AMT_SCORE, BUP_FY11_SLS_AMT_SCORE, 
                         BUP_FY12_SLS_AMT_SCORE, BUP_FY13_P_SLS_AMT_SCORE, BUP_FY13_A_SLS_AMT_SCORE, BUP_FY14_P_SLS_AMT_SCORE, 
                         BUP_FY14_A_SLS_AMT_SCORE, BUP_FY15_SLS_AMT_SCORE, BUP_FY16_SLS_AMT_SCORE, BUP_FY17_SLS_AMT_SCORE, 
                         BUP_FY18_SLS_AMT_SCORE, BUP_NOTES_SCORE, BUP_DETL_SCORE, BUP_ISSUES_SCORE, BUP_DETL_STAT_SCORE, 
                         ABP_CUST_STRAT, ABP_EXEC_ASKS, ABP_HP_CUST_STATE, ABP_KEY_ISSUE, ABP_STRAT_OPP, OPP_HPSOLUTION, 
                         OPP_NAME, OPP_NM, CRM_TITLE, CRM_ADDTNL_INFO, CRM_BUCONTACT, OPP_PRI_CHNLPARTN_ACCT, OPP_PRI_CAMP_NM, 
                         OPP_PRI_CAMP_SOURCE, OPP_PRI_COMP, OPP_OWNER, OPP_OWNER_NM, CREATOR, OPP_LAST_MOD_NM),
                                     
         ifelse(data_name=="bus", mydata1 <- mydata1 %>% 
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 80),
                         BUP_ID = substr(BUP_ID, 1, 15),
                         BUP_NM = substr(BUP_NM, 1, 63),
                         BUS_NM = substr(BUS_NM, 1, 81),
                         BUS_ID = substr(BUS_ID, 1, 15),
                         BUS_CHALLENGE = substr(BUS_CHALLENGE, 1, 256),
                         BUS_CREATOR = substr(BUS_CREATOR, 1, 29),
                         BUS_CURRENCY = substr(BUS_CURRENCY, 1, 3),
                         BUS_CUST_IT_PRIOR_ADDRSD = substr(BUS_CUST_IT_PRIOR_ADDRSD, 1, 256),
                         BUS_EXECTN_ACT = substr(BUS_EXECTN_ACT, 1, 257),
                         BUS_HP_DIFF = substr(BUS_HP_DIFF, 1, 256),
                         BUS_COMP_REACT = substr(BUS_COMP_REACT, 1, 256),
                         BUS_LAST_MOD_NM = substr(BUS_LAST_MOD_NM, 1, 29),
                         BUS_PARTN_INFLNCE_STRAT = substr(BUS_PARTN_INFLNCE_STRAT, 1, 256),
                         BUS_REQ_PROC_CHANGE = substr(BUS_REQ_PROC_CHANGE, 1, 256),
                         BUS_TCV_CURRENCY = substr(BUS_TCV_CURRENCY, 1, 3),
                         BUS_TCV_CONV = substr(BUS_TCV_CONV, 1, 3),
                         BUS_SOLUTION_COMPONENTS = substr(BUS_SOLUTION_COMPONENTS, 1, 249),
                         BUS_TIMING = substr(BUS_TIMING, 1, 20),
                         BUS_BU_SOLTN = substr(BUS_BU_SOLTN, 1, 256),
                         BUS_TYPE = substr(BUS_TYPE, 1, 50)),
                                            
         ifelse(data_name=="compls", mydata1 <- mydata1 %>% 
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 80),
                         CL_ID = substr(CL_ID, 1, 15),
                         CL_COMMENTS = substr(CL_COMMENTS, 1, 256),
                         CL_BUS_AREA = substr(CL_BUS_AREA, 1, 71),
                         CL_BUS_GRP = substr(CL_BUS_GRP, 1, 5),
                         CL_BUS_UNIT = substr(CL_BUS_UNIT, 1, 40),
                         CL_COMP1 = substr(CL_COMP1, 1, 43),
                         CL_COMP2 = substr(CL_COMP2, 1, 43),
                         CL_COMP3 = substr(CL_COMP3, 1, 43),
                         CL_NEXT_STEP = substr(CL_NEXT_STEP, 1, 23),
                         CL_SUMMARY = substr(CL_SUMMARY, 1, 35),
                         CL_CURRENCY = substr(CL_CURRENCY, 1, 3),
                         CL_CREATOR = substr(CL_CREATOR, 1, 28),
                         CL_CREATOR_ALIAS = substr(CL_CREATOR_ALIAS, 1, 8),
                         CL_LAST_MOD_NM = substr(CL_LAST_MOD_NM, 1, 28),
                         CL_LAST_MOD_ALIAS = substr(CL_LAST_MOD_ALIAS, 1, 8),
                         CL_LAST_ACTIVITY_DT = substr(CL_LAST_ACTIVITY_DT, 1, 1)),
                                                   
         ifelse(data_name=="custprior", mydata1 <- mydata1 %>% 
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 80),
                         CBP_ID = substr(CBP_ID, 1, 15),
                         CBP_NM = substr(CBP_NM, 1, 81),
                         CBP_DETL_DESC = substr(CBP_DETL_DESC, 1, 2949),
                         CBP_EXP_OUTCOME = substr(CBP_EXP_OUTCOME, 1, 699),
                         CBP_PRIOR = substr(CBP_PRIOR, 1, 6),
                         CBP_DESC = substr(CBP_DESC, 1, 82),
                         CBP_TIMING = substr(CBP_TIMING, 1, 20),
                         CBP_CURRENCY = substr(CBP_CURRENCY, 1, 3),
                         CBP_CREATOR = substr(CBP_CREATOR, 1, 28),
                         CBP_CREATOR_ALIAS = substr(CBP_CREATOR_ALIAS, 1, 8),
                         CBP_LAST_MOD_NM = substr(CBP_LAST_MOD_NM, 1, 28),
                         CBP_LAST_MOD_ALIAS = substr(CBP_LAST_MOD_ALIAS, 1, 8),
                         CBP_LAST_ACTIVITY_DT = substr(CBP_LAST_ACTIVITY_DT, 1, 1)),       
                                                          
         ifelse(data_name=="opp", mydata1 <- mydata1 %>% 
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 81),
                         HP_SI_ID = substr(HP_SI_ID, 1, 15),
                         OPP_CREATOR = substr(OPP_CREATOR, 1, 28),
                         OPP_ID = substr(OPP_ID, 1, 14),
                         OPP_HPSOLUTION = substr(OPP_HPSOLUTION, 1, 28),
                         OPP_NM = substr(OPP_NM, 1, 117),
                         OPP_CLOSE_DT = substr(OPP_CLOSE_DT, 1, 50),
                         OPP_OWNER_NM = substr(OPP_OWNER_NM, 1, 31),
                         OPPN_REC_TYPE = substr(OPPN_REC_TYPE, 1, 8),
                         OPP_SLS_STAGE = substr(OPP_SLS_STAGE, 1, 28),
                         OPP_OWNER = substr(OPP_OWNER, 1, 31),
                         OPP_PRIOR_SLS_STAGE = substr(OPP_PRIOR_SLS_STAGE, 1, 28),
                         OPP__PRI_PARTN_ACCT = substr(OPP__PRI_PARTN_ACCT, 1, 1),
                         OPP_PRI_CAMP_NM = substr(OPP_PRI_CAMP_NM, 1, 78),
                         OPP_PRI_CAMP_SOURCE = substr(OPP_PRI_CAMP_SOURCE, 1, 78),
                         OPP_PRI_CHNLPARTN_ACCT = substr(OPP_PRI_CHNLPARTN_ACCT, 1, 90),
                         OPP_PRI_COMP = substr(OPP_PRI_COMP, 1, 43),
                         OPP_FORECAST_CAT = substr(OPP_FORECAST_CAT, 1, 8),
                         OPP_GOTOMARKET_ROUTE = substr(OPP_GOTOMARKET_ROUTE, 1, 8),
                         OPP_CURRENCY = substr(OPP_CURRENCY, 1, 3),
                         OPP_TTL_VAL_CURRENCY = substr(OPP_TTL_VAL_CURRENCY, 1, 3),
                         OPP_TTL_CON_CURRENCY = substr(OPP_TTL_CON_CURRENCY, 1, 3)) %>%
                  
                  select(SNAP_DT, ACCT_PLAN_ID, ACCT_PLAN_NM, HP_SI_ID, OPP_CLOSE_DT, OPP_CREATOR, OPP_CREATED_DT,
                         OPP_ID, OPP_HPSOLUTION, OPP_NM, OPP_OWNER_NM, OPPN_REC_TYPE, OPP_SLS_STAGE, OPP_OWNER,
                         OPP_PRIOR_SLS_STAGE, OPP__PRI_PARTN_ACCT, OPP_PRI_CAMP_NM, OPP_PRI_CAMP_SOURCE,
                         OPP_PRI_CHNLPARTN_ACCT, OPP_PRI_COMP, OPP_FORECAST_CAT, OPP_GOTOMARKET_ROUTE, OPP_CURRENCY,
                         OPP_TTL_VAL_CURRENCY, OPP_TTL_VALUE, OPP_TTL_CON_CURRENCY, OPP_TTL_CON, OPP_STAT, OPP_TTL_CON_S0,
                         OPP_PRI_CAMP_NM_LEN, OPP_PRI_CAMP_SRC_LEN, OPP_HPSOLUTION_LEN, OPP_PRI_PARTN_LEN, OPP_PRI_COMP_LEN,
                         OPP_TTL_VAL_SCORE, OPP_FORECAST_SCORE, OPP_PRI_CAMP_NM_SCORE, OPP_PRI_CAMP_SRC_SCORE, OPP_HPSOLUTION_SCORE,
                         OPP_PRI_PARTN_SCORE, OPP_PRI_COMP_SCORE, BUP_DETL, BUP_DETL_STAT, BUP_ISSUES, BUP_KEY_CONTACT,
                         BUP_NOTES, BUP_PPT_ITG_DESC, ABP_CUST_STRAT, ABP_EXEC_ASKS, ABP_HP_CUST_STATE, ABP_KEY_ISSUE, 
                         ABP_STRAT_OPP, OPP_NAME, BUP_PLAN_NM, CRM_TITLE, CRM_ADDTNL_INFO, CRM_BUCONTACT, CREATOR, OPP_LAST_MOD_NM),   
                                                                 
         ifelse(data_name=="partnerlandscape", mydata1 <- mydata1 %>% 
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 81),
                         PARTN_ACCT_NM = substr(PARTN_ACCT_NM, 1, 83),
                         PARTN_ID = substr(PARTN_ID, 1, 15),
                         PARTN_ALLIANCE = substr(PARTN_ALLIANCE, 1, 3),
                         PARTN_CHNNL = substr(PARTN_CHNNL, 1, 3),
                         PARTN_CREATOR_NM = substr(PARTN_CREATOR_NM, 1, 25),
                         PARTN_CURRENCY = substr(PARTN_CURRENCY, 1, 3),
                         PARTN_HP_STRAT = substr(PARTN_HP_STRAT, 1, 1042),
                         PARTN_MGR_NM = substr(PARTN_MGR_NM, 1, 21),
                         PARTN_LAST_MOD_NM = substr(PARTN_LAST_MOD_NM, 1, 27),
                         PARTN_FOCUS = substr(PARTN_FOCUS, 1, 885),
                         PARTN_INFLUENCE = substr(PARTN_INFLUENCE, 1, 7),
                         PARTN_SEQ = substr(PARTN_SEQ, 1, 8),
                         PARTNER_TYPE = substr(PARTNER_TYPE, 1, 16)),       
                                                                        
         ifelse(data_name=="scorecard", mydata1 <- mydata1 %>%
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 100),
                         SC_ID = substr(SC_ID, 1, 15),
                         SC_NM = substr(SC_NM, 1, 30),
                         SC_ACCT_SUMRY_ANSWER = substr(SC_ACCT_SUMRY_ANSWER, 1, 200),
                         SC_SUMRY_COMMENTS = substr(SC_SUMRY_COMMENTS, 1, 250),
                         SC_SUMRY_ADTNL = substr(SC_SUMRY_ADTNL, 1, 500),
                         SC_BUP_A1 = substr(SC_BUP_A1, 1, 1),
                         SC_BUP_A2 = substr(SC_BUP_A2, 1, 1),
                         SC_BUP_A3 = substr(SC_BUP_A3, 1, 1),
                         SC_BUP_A4 = substr(SC_BUP_A4, 1, 1),
                         SC_BUP_A5 = substr(SC_BUP_A5, 1, 1),
                         SC_BUP_C1 = substr(SC_BUP_C1, 1, 1),
                         SC_BUP_ANSWER = substr(SC_BUP_ANSWER, 1, 50),
                         SC_BUP_COMMENTS = substr(SC_BUP_COMMENTS, 1, 300),
                         SC_COMPLS_ANSWER = substr(SC_COMPLS_ANSWER, 1, 50),
                         SC_COMPLS_COMMENTS = substr(SC_COMPLS_COMMENTS, 1, 200),
                         SC_CBP_ANSWER = substr(SC_CBP_ANSWER, 1, 100),
                         SC_CBP_COMMENTS = substr(SC_CBP_COMMENTS, 1, 250),
                         SC_CR_A1 = substr(SC_CR_A1, 1, 50),
                         SC_CR_A2 = substr(SC_CR_A2, 1, 50),
                         SC_CR_A3 = substr(SC_CR_A3, 1, 50),
                         SC_CR_A4 = substr(SC_CR_A4, 1, 50),
                         SC_CR_COMMENTS1 = substr(SC_CR_COMMENTS1, 1, 300),
                         SC_CR_COMMENTS3 = substr(SC_CR_COMMENTS3, 1, 300),
                         SC_CR_COMMENTS4 = substr(SC_CR_COMMENTS4, 1, 300),
                         SC_CR_COMMENTS2 = substr(SC_CR_COMMENTS2, 1, 300),
                         SC_HPSI_A1 = substr(SC_HPSI_A1, 1, 200),
                         SC_HPSI_A2 = substr(SC_HPSI_A2, 1, 200),
                         SC_HPSI_COMMENTS1 = substr(SC_HPSI_COMMENTS1, 1, 250),
                         SC_HPSI_COMMENTS2 = substr(SC_HPSI_COMMENTS2, 1, 250),
                         SC_IA_ANSWER = substr(SC_IA_ANSWER, 1, 50),
                         SC_IA_COMMENTS = substr(SC_IA_COMMENTS, 1, 200),
                         SC_ABP_SCORECARD_CURRENCY = substr(SC_ABP_SCORECARD_CURRENCY, 1, 3),
                         SC_CREATOR = substr(SC_CREATOR, 1, 50),
                         SC_CREATED_ALIAS = substr(SC_CREATED_ALIAS, 1, 10),
                         SC_LAST_MOD_NM = substr(SC_LAST_MOD_NM, 1, 50),
                         SC_LAST_MOD_ALIAS = substr(SC_LAST_MOD_ALIAS, 1, 10)),
                                                                               
         ifelse(data_name=="stratinit", mydata1 <- mydata1 %>% 
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 80),
                         SI_ID = substr(SI_ID, 1, 15),
                         SI_NM = substr(SI_NM, 1, 80),
                         SI_CUST_BUS_CASE = substr(SI_CUST_BUS_CASE, 1, 259),
                         SI_CUST_BUS_PRIOR = substr(SI_CUST_BUS_PRIOR, 1, 1),
                         SI_CUST_SPONSOR = substr(SI_CUST_SPONSOR, 1, 29),
                         SI_DESC = substr(SI_DESC, 1, 258),
                         SI_EXECTN_TIMEFRAME = substr(SI_EXECTN_TIMEFRAME, 1, 20),
                         SI_EXECTN_STAT = substr(SI_EXECTN_STAT, 1, 11),
                         SI_FUNDG_STAT = substr(SI_FUNDG_STAT, 1, 11),
                         SI_SOLUTION = substr(SI_SOLUTION, 1, 181),
                         SI_HP_SPONSOR_NM = substr(SI_HP_SPONSOR_NM, 1, 32),
                         SI_INNOV_INIT = substr(SI_INNOV_INIT, 1, 1),
                         SI_NON_DISCLSR_AGRMT = substr(SI_NON_DISCLSR_AGRMT, 1, 1),
                         SI_PROJ_INIT_CUR = substr(SI_PROJ_INIT_CUR, 1, 3),
                         SI_PROJ_INIT_CONV = substr(SI_PROJ_INIT_CONV, 1, 3),
                         SI_DEPENDENCIES = substr(SI_DEPENDENCIES, 1, 256),
                         SI_RISK = substr(SI_RISK, 1, 256),
                         SI_CURRENCY = substr(SI_CURRENCY, 1, 3),
                         SI_CREATOR_NM = substr(SI_CREATOR_NM, 1, 26),
                         SI_CREATOR_ALIAS = substr(SI_CREATOR_ALIAS, 1, 8),
                         SI_LAST_MOD_NM = substr(SI_LAST_MOD_NM, 1, 26),
                         SI_LAST_MOD_ALIAS = substr(SI_LAST_MOD_ALIAS, 1, 8)),       
                                                                                      
         ifelse(data_name=="tce", mydata1 <- mydata1 %>% 
                  mutate(ACCT_PLAN_ID = substr(ACCT_PLAN_ID, 1, 15),
                         ACCT_PLAN_NM = substr(ACCT_PLAN_NM, 1, 100),
                         TCE_ADDTNL_INFO = substr(TCE_ADDTNL_INFO, 1, 2000),
                         TCE_CREATOR = substr(TCE_CREATOR, 1, 50),
                         TCE_CURRENCY = substr(TCE_CURRENCY, 1, 3),
                         TCE_CUR_SCORE = substr(TCE_CUR_SCORE, 1, 50),
                         TCE_METRICS = substr(TCE_METRICS, 1, 50),
                         TCE_LAST_MOD_NM = substr(TCE_LAST_MOD_NM, 1, 50),
                         TCE_CUST_CONCERNS = substr(TCE_CUST_CONCERNS, 1, 2000),
                         TCE_PREV_SCORE = substr(TCE_PREV_SCORE, 1, 50),
                         TCE_STATUS = substr(TCE_STATUS, 1, 25),
                         TCE_STRATEGY = substr(TCE_STRATEGY, 1, 2000),
                         TCE_CODE = substr(TCE_CODE, 1, 10),
                         TCE_ID = substr(TCE_ID, 1, 15)), 
                print(data_name) & stop("File name is not recognized!")
                )))))))))))))
  
  
  # Change bup report name to bup_FY15 to match QV model
  ifelse(data_name=="bup", 
         filename <- paste(data_name, "_FY15.txt", sep=""),
         filename <- paste(data_name, ".txt", sep=""))
  
  # Export data to txt format using | as delimiter
  write.table(mydata1, file = filename, sep = "|", row.names=FALSE, quote=FALSE, na = "")
}

setwd("/home/zhaoch/R_Workspace/Acct Growth/Data/output")

Process_Export(abp)
Process_Export(acct)
Process_Export(acctcustomercontact)
Process_Export(bugsoppty)
Process_Export(bup)
Process_Export(bus)
Process_Export(compls)
Process_Export(custprior)
Process_Export(opp)
Process_Export(partnerlandscape)
Process_Export(scorecard)
Process_Export(stratinit)
Process_Export(tce)


# # Show the current SNAP_DT list.
# test <- abp %>% select(SNAP_DT) %>% 
#   filter((as.Date(SNAP_DT,"%d%b%Y") >= End_Month(as.Date(SNAP_DT,"%d%b%Y"))-6 & 
#                                               as.Date(SNAP_DT,"%d%b%Y") <= End_Month(as.Date(SNAP_DT,"%d%b%Y"))) | 
#                                              as.Date(SNAP_DT,"%d%b%Y") + 23 > as.Date(current_day,"%d%b%Y") |
#                                              SNAP_DT %in% c("10DEC2015") |
#                                              SNAP_DT %in% c("22NOV2016") |
#                                              SNAP_DT %in% c("21DEC2016")) %>%
#   filter(!SNAP_DT %in% c("27SEP2013", "25OCT2013", "29NOV2013", "27DEC2013", "30JAN2014", "27FEB2014", "27MAR2014", 
#                          "24APR2014", "29MAY2014", "27NOV2014", "18DEC2014", "29JAN2015", "26FEB2015", "27MAR2015", 
#                          "30APR2015", "29MAY2015", "25JUN2015", "31JUL2015", "27AUG2015", "24SEP2015", "29OCT2015",
#                          "25NOV2015", "10DEC2015")) %>%
#   unique() %>% 
#   as.character()
# 
# test

