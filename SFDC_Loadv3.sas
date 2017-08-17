/***********************************************************************\
*Program Name:	SFDC_Loadv3.sas     									*
*Creator: 		Liang Feng, Chunyou Zhao								*
*Date:    		Oct 1st, 2013											*
*Description: 	This program is used to load the SFDC data from xls		*
*				to the sas datasets then prepare file for QV reports	*
\***********************************************************************/

*********************************************************************************************************************************;
*********************************************************************************************************************************;
**************Step 1 Import data from xls********************************;
*********************************************************************************************************************************;
*********************************************************************************************************************************;

/*Importing Data into SAS: Need to copy the xls into the Prowler server*/
%let fpath=/var/blade/data2031/esiblade/AcctGrowth/Data/;
*%let snap_dt=20160422;
%let snap_dt=%sysfunc(today(),yymmddN8.);


%macro xlsxloads(sasname=, xlsname=);
proc import out=work.&sasname. datafile="&fpath.V&snap_dt./&xlsname..xlsx" dbms=xlsx replace;
getnames=yes;
run;
%mend xlsxloads;

options mprint;

%let sfilenm1=abp&snap_dt.;
%let sfilenm2=acct&snap_dt.;
%let sfilenm3=bup&snap_dt;
%let sfilenm4=compls&snap_dt.;
%let sfilenm5=custprior&snap_dt.;
%let sfilenm6=partnerls&snap_dt.;
%let sfilenm7=opp&snap_dt.;
%let sfilenm8=stratinit&snap_dt.;
%let sfilenm9=bus&snap_dt.;
%let sfilenm10=tce&snap_dt.;
%let sfilenm11=scorecard&snap_dt.;
*%let sfilenm12=bugsstratinit;
%let sfilenm13=bugsoppty&snap_dt.;
%let sfilenm14=acctcustomercontact&snap_dt.;
%let tfilenm1=abp;
%let tfilenm2=acct;
%let tfilenm3=bup;
%let tfilenm4=compls;
%let tfilenm5=custprior;
%let tfilenm6=partnerlandscape;
%let tfilenm7=opp;
%let tfilenm8=stratinit;
%let tfilenm9=bus;
%let tfilenm10=tce;
%let tfilenm11=scorecard;
*%let tfilenm12=bugssi;
%let tfilenm13=bugsoppty;
%let tfilenm14=acctcustomercontact;

%xlsxloads(sasname=&tfilenm1., xlsname=&sfilenm1.);
%xlsxloads(sasname=&tfilenm2., xlsname=&sfilenm2.);
%xlsxloads(sasname=&tfilenm3., xlsname=&sfilenm3.);
%xlsxloads(sasname=&tfilenm4., xlsname=&sfilenm4.);
%xlsxloads(sasname=&tfilenm5., xlsname=&sfilenm5.);
%xlsxloads(sasname=&tfilenm6., xlsname=&sfilenm6.);
%xlsxloads(sasname=&tfilenm7., xlsname=&sfilenm7.);
%xlsxloads(sasname=&tfilenm8., xlsname=&sfilenm8.);
%xlsxloads(sasname=&tfilenm9., xlsname=&sfilenm9.);
%xlsxloads(sasname=&tfilenm10., xlsname=&sfilenm10.);
%xlsxloads(sasname=&tfilenm11., xlsname=&sfilenm11.);
*%xlsxloads(sasname=&tfilenm12., xlsname=&sfilenm12.);
%xlsxloads(sasname=&tfilenm13., xlsname=&sfilenm13.);
%xlsxloads(sasname=&tfilenm14., xlsname=&sfilenm14.);


*********************************************************************************************************************************;
*********************************************************************************************************************************;
**************Step 2 put in the libary for the new data**********************;
*********************************************************************************************************************************;
*********************************************************************************************************************************;

*Copying the imported data, handling the data type and Adding the timestamps;
libname AcctGrow  "&fpath.V&snap_dt.";

%macro handledate(indate=,outdate=);
if compress(&indate,,'wk') ne '' then do;
mth = input(scan(&indate,1, '/'), 2.);
dy = input(scan(&indate,2, '/'), 2.);
yr = input(scan(&indate,3, '/'), 4.);
if index(&indate, '/') > 0 then &outdate = MDY(mth, dy, yr);
else &outdate  = input(trim(&indate), 5.) - 21916;
end;
else &outdate = . ;
%mend handledate;


data AcctGrow.ABP(compress=yes);
format snap_dt date9.;
set work.ABP (rename=(ABP_HP_SI_SCORE=HPSISCORE)) nobs=_nobs;
snap_dt = mdy(substr("&snap_dt.", 5, 2), substr("&snap_dt.", 7, 2), substr("&snap_dt.", 1, 4))  ;
ABP_HP_SI_SCORE = compress(HPSISCORE,'','wk') * 1;
*Remove last 6 rows that have SFDC auto generated trailer info;
if _n_ le _nobs-6;
drop hpsiscore;
run;

data AcctGrow.ACCT(compress=yes);
format snap_dt date9.;
set work.ACCT(rename=(ACCT_LASTGEOASSIGN_DT=OLD_LASTGEOASSIGN_DT
ACCT_LAST_PROF_DT=old_LAST_PROF_DT ACCT_LAST_COVRG_DT = OLD_LAST_COVRG_DT
ACCT_LAST_IND_DT = OLD_LAST_IND_DT ACCT_MDCP_LAST_SYNC=OLD_MDCP_LAST_SYNC
ACCT_RPL_LAST_SYNC = OLD_RPL_LAST_SYNC
)) nobs=_nobs;
snap_dt = mdy(substr("&snap_dt.", 5, 2), substr("&snap_dt.", 7, 2), substr("&snap_dt.", 1, 4))  ;

ACCT_LASTGEOASSIGN_DT=DATEPART(OLD_LASTGEOASSIGN_DT);
ACCT_LAST_PROF_DT=DATEPART(old_LAST_PROF_DT);
ACCT_LAST_COVRG_DT = DATEPART(OLD_LAST_COVRG_DT);
ACCT_LAST_IND_DT = DATEPART(OLD_LAST_IND_DT);
ACCT_MDCP_LAST_SYNC=DATEPART(OLD_MDCP_LAST_SYNC);
ACCT_RPL_LAST_SYNC = DATEPART(OLD_RPL_LAST_SYNC);

drop  OLD_LASTGEOASSIGN_DT old_LAST_PROF_DT OLD_LAST_COVRG_DT OLD_LAST_IND_DT OLD_MDCP_LAST_SYNC OLD_RPL_LAST_SYNC;
format  ACCT_LASTGEOASSIGN_DT ACCT_LAST_PROF_DT ACCT_LAST_COVRG_DT ACCT_LAST_IND_DT ACCT_MDCP_LAST_SYNC ACCT_RPL_LAST_SYNC date9.;
if _n_ le _nobs-6;
run;

data AcctGrow.BUP(compress=yes);
format snap_dt date9.;
set work.BUP nobs=_nobs;
snap_dt = mdy(substr("&snap_dt.", 5, 2), substr("&snap_dt.", 7, 2), substr("&snap_dt.", 1, 4))  ;
if _n_ le _nobs-6;
run;

data AcctGrow.COMPLS(compress=yes);
format snap_dt date9.;
set work.COMPLS nobs=_nobs;
snap_dt = mdy(substr("&snap_dt.", 5, 2), substr("&snap_dt.", 7, 2), substr("&snap_dt.", 1, 4))  ;
if _n_ le _nobs-6;
run;

data AcctGrow.CUSTPRIOR(compress=yes);
format snap_dt date9.;
set work.CUSTPRIOR nobs=_nobs;
snap_dt = mdy(substr("&snap_dt.", 5, 2), substr("&snap_dt.", 7, 2), substr("&snap_dt.", 1, 4))  ;
if _n_ le _nobs-6;
run;

data AcctGrow.OPP(compress=yes);
format snap_dt date9.;
set work.OPP nobs=_nobs;
snap_dt = mdy(substr("&snap_dt.", 5, 2), substr("&snap_dt.", 7, 2), substr("&snap_dt.", 1, 4))  ;
if _n_ le _nobs-6;
run;


data AcctGrow.PARTNERLANDSCAPE(compress=yes);
format snap_dt date9.;
set work.PARTNERLANDSCAPE nobs=_nobs; 
snap_dt = mdy(substr("&snap_dt.", 5, 2), substr("&snap_dt.", 7, 2), substr("&snap_dt.", 1, 4))  ;
if _n_ le _nobs-6;
run;

data AcctGrow.STRATINIT(compress=yes);
format snap_dt date9.;
set work.STRATINIT nobs=_nobs;
snap_dt = mdy(substr("&snap_dt.", 5, 2), substr("&snap_dt.", 7, 2), substr("&snap_dt.", 1, 4))  ;
if _n_ le _nobs-6;
run;

data AcctGrow.BUS(compress=yes);
format snap_dt date9.;
set work.BUS nobs=_nobs;
snap_dt = mdy(input(substr("&snap_dt.", 5, 2),2.0) , input(substr("&snap_dt.", 7, 2), 2.0), input(substr("&snap_dt.", 1, 4), 4.0))  ;
if _n_ le _nobs-6;
run;

data AcctGrow.TCE(compress=yes);
format snap_dt date9.;
set work.TCE nobs=_nobs;
snap_dt = mdy(input(substr("&snap_dt.", 5, 2),2.0) , input(substr("&snap_dt.", 7, 2), 2.0), input(substr("&snap_dt.", 1, 4), 4.0))  ;
if _n_ le _nobs-6;
run;

data AcctGrow.SCORECARD(compress=yes);
format snap_dt date9.;
set work.SCORECARD nobs=_nobs;
snap_dt = mdy(input(substr("&snap_dt.", 5, 2),2.0) , input(substr("&snap_dt.", 7, 2), 2.0), input(substr("&snap_dt.", 1, 4), 4.0))  ;
if _n_ le _nobs-6;
run;

*data liang.BUGSSI(compress=yes);
*format snap_dt date9.;
*set work.bugssi;
*snap_dt = mdy(input(substr("&snap_dt.", 5, 2),2.0) , input(substr("&snap_dt.", 7, 2), 2.0), input(substr("&snap_dt.", 1, 4), 4.0))  ;
*where acct_plan_id like 'a0iG%';
*run;

data AcctGrow.bugsoppty(compress=yes);
format snap_dt date9.;
set work.bugsoppty nobs=_nobs;
snap_dt = mdy(input(substr("&snap_dt.", 5, 2),2.0) , input(substr("&snap_dt.", 7, 2), 2.0), input(substr("&snap_dt.", 1, 4), 4.0))  ;
if _n_ le _nobs-6;
run;

data AcctGrow.acctcustomercontact(compress=yes);
format snap_dt date9.;
set work.acctcustomercontact nobs=_nobs;
snap_dt = mdy(input(substr("&snap_dt.", 5, 2),2.0) , input(substr("&snap_dt.", 7, 2), 2.0), input(substr("&snap_dt.", 1, 4), 4.0))  ;
if _n_ le _nobs-6;
run;

*compress;
%macro compressdata (dsname=);
data AcctGrow.&dsname(compress=yes);
set  AcctGrow.&dsname;
array col $ _CHARACTER_;
do i=1 to dim(col);
col{i}=compress(col{i},,'c');
end;
drop i;
run;
%mend compressdata;

options mprint mlogic symbolgen;

%compressdata(dsname=ABP);
%compressdata(dsname=ACCT);
%compressdata(dsname=BUP);
%compressdata(dsname=COMPLS);
%compressdata(dsname=CUSTPRIOR);
%compressdata(dsname=PARTNERLANDSCAPE);
%compressdata(dsname=opp);
%compressdata(dsname=stratinit);
%compressdata(dsname=bus);
%compressdata(dsname=tce);
%compressdata(dsname=scorecard);
*%compressdata(dsname=bugssi);
%compressdata(dsname=bugsoppty);
%compressdata(dsname=acctcustomercontact);


*********************************************************************************************************************************;
*********************************************************************************************************************************;
**************Step 3 Appending to Archive data****************************************;
*********************************************************************************************************************************;
*********************************************************************************************************************************;

/*appending Imported Data into Archive*/
libname ARC  "/var/blade/data2031/esiblade/AcctGrowth/Data/arc";
**clean up if we need to rerun the appending otherwise it will have dups;
%macro cleanup(ds=, dt=);
data arc.&ds (compress=yes);
set arc.&ds;
if snap_dt eq mdy(input(substr("&dt.", 5, 2),2.0) , input(substr("&dt.", 7, 2), 2.0), input(substr("&dt.", 1, 4), 4.0)) then delete;
run;
%mend cleanup;

%cleanup(ds=ABP, dt=&snap_dt.);
%cleanup(ds=ACCT, dt=&snap_dt.);
%cleanup(ds=BUP, dt=&snap_dt.);
%cleanup(ds=COMPLS, dt=&snap_dt.);
%cleanup(ds=CUSTPRIOR, dt=&snap_dt.);
%cleanup(ds=PARTNERLANDSCAPE, dt=&snap_dt.);
%cleanup(ds=opp, dt=&snap_dt.);
%cleanup(ds=stratinit, dt=&snap_dt.);
%cleanup(ds=bus, dt=&snap_dt.);
%cleanup(ds=tce, dt=&snap_dt.);
%cleanup(ds=scorecard, dt=&snap_dt.);
%cleanup(ds=bugsoppty, dt=&snap_dt.);
%cleanup(ds=acctcustomercontact, dt=&snap_dt.);

*appending;
libname AcctGrow  "&fpath.V&snap_dt.";


proc append base=ARC.ABP data=AcctGrow.ABP force;
run;

proc append base=ARC.ACCT data=AcctGrow.ACCT force;
run;

proc append base=ARC.BUP data=AcctGrow.BUP force;
run;

proc append base=ARC.COMPLS data=AcctGrow.COMPLS force;
run;

proc append base=ARC.CUSTPRIOR data=AcctGrow.CUSTPRIOR force;
run;

proc append base=ARC.PARTNERLANDSCAPE data=AcctGrow.PARTNERLANDSCAPE force;
run;

proc append base=ARC.opp data=AcctGrow.opp force;
run;

proc append base=ARC.stratinit data=AcctGrow.stratinit force;
run;

proc append base=ARC.bus data=AcctGrow.bus force;
run;

proc append base=ARC.TCE data=AcctGrow.TCE force;
run;

proc append base=ARC.scorecard data=AcctGrow.scorecard force;
run;

proc append base=ARC.bugsoppty data=AcctGrow.bugsoppty force;
run;

proc append base=ARC.acctcustomercontact data=AcctGrow.acctcustomercontact force;
run;


*********************************************************************************************************************************;
*********************************************************************************************************************************;
**************Step 4 calculate the derived fields**********************;
*********************************************************************************************************************************;
*********************************************************************************************************************************;

*Processing the data, calculating and adding new Elements; 
libname ARC  "/var/blade/data2031/esiblade/AcctGrowth/Data/arc";


data arc.abp;
set arc.abp;

if length(compress(ABP_CUST_STRAT,,'wk')) > 500 then score1=0.25; 
else if length(compress(ABP_CUST_STRAT,,'wk')) >= 200 then score1=0.15; 
else score1=0.1;

if length(compress(ABP_HP_CUST_STATE,,'wk')) > 500 then score2=0.25; 
else if length(compress(ABP_HP_CUST_STATE,,'wk')) >= 200 then score2=0.15; 
else score2=0.1;

if length(compress(ABP_STRAT_OPP,,'wk')) > 500 then score3=0.25; 
else if length(compress(ABP_STRAT_OPP,,'wk')) >= 200 then score3=0.15 ;
else score3=0.1;

if length(compress(ABP_KEY_ISSUE,,'wk')) > 500 then score4=0.25; 
else if length(compress(ABP_KEY_ISSUE,,'wk')) >= 200 then score4=0.15;
else score4=0.1;

ABP_COMPL_SCORE=score1+score2+score3+score4;
format ABP_COMPL_SCORE 4.2;
drop score1-score4;

if ABP_EXEC_ASKS ne '' then ABP_EXEC_ASKS_FLAG='Yes';
else ABP_EXEC_ASKS_FLAG = 'No';

if length(compress(ABP_CUST_DFN_INNVTN,,'wk')) gt 400 then do;
score1 = 0.3;
end;
else if length(compress(ABP_CUST_DFN_INNVTN,,'wk')) ge 200  then do;
score1 = 0.2;
end;
else do;
score1 = 0.1;
end;

if length(compress(ABP_HP_INNVTN_STRAT,,'wk')) gt 400 then do;
score2 = 0.3;
end;
else if length(compress(ABP_HP_INNVTN_STRAT,,'wk')) ge 200  then do;
score2 = 0.2;
end;
else do;
score2 = 0.1;
end;

if  ABP_LAST_CUST_ENGGMNT gt snap_dt - 90 then do;
score3 = 0.4;
end;
else do;
score3 = 0.0;
end;
ABP_IA_Compl_Score=score1+score2+score3;
format ABP_IA_Compl_Score 4.2;
drop score1-score3;

if Missing(ABP_LAST_CUST_ENGGMNT) then ABP_LAST_CUST_ENGGMNT_FLAG = 'Missing';
else if intck('month', ABP_LAST_CUST_ENGGMNT, snap_dt)>9 then ABP_LAST_CUST_ENGGMNT_FLAG = 'Red    ';
else if intck('month', ABP_LAST_CUST_ENGGMNT, snap_dt)> 6 then ABP_LAST_CUST_ENGGMNT_FLAG = 'Amber  ';
else ABP_LAST_CUST_ENGGMNT_FLAG ='Green  ';

if Missing(ABP_NEXT_CUST_ENGGMNT) then ABP_NEXT_CUST_ENGGMNT_FLAG = 'Red  ';
else if not Missing(ABP_LAST_CUST_ENGGMNT) and intck('month', ABP_LAST_CUST_ENGGMNT, ABP_NEXT_CUST_ENGGMNT)> 6 then ABP_NEXT_CUST_ENGGMNT_FLAG = 'Red ';
else if intck('month', snap_dt, ABP_NEXT_CUST_ENGGMNT)> 6 then ABP_NEXT_CUST_ENGGMNT_FLAG = 'Amber';
else if not Missing(ABP_LAST_CUST_ENGGMNT) and intck('month', ABP_LAST_CUST_ENGGMNT, ABP_NEXT_CUST_ENGGMNT)> 3 then ABP_NEXT_CUST_ENGGMNT_FLAG = 'Amber';
else if intck('month', snap_dt, ABP_NEXT_CUST_ENGGMNT)>= 0 then ABP_NEXT_CUST_ENGGMNT_FLAG ='Green';
else if not Missing(ABP_LAST_CUST_ENGGMNT) and intck('month', ABP_LAST_CUST_ENGGMNT, ABP_NEXT_CUST_ENGGMNT)>=0 then ABP_NEXT_CUST_ENGGMNT_FLAG = 'Green';
else if intck('month', snap_dt, ABP_NEXT_CUST_ENGGMNT) < 0 then ABP_NEXT_CUST_ENGGMNT_FLAG ='N/A  ';
else if Missing(ABP_LAST_CUST_ENGGMNT) or intck('month', ABP_LAST_CUST_ENGGMNT, ABP_NEXT_CUST_ENGGMNT) < 0 then ABP_NEXT_CUST_ENGGMNT_FLAG ='N/A  ';
else ABP_NEXT_CUST_ENGGMNT_FLAG ='Other';

if ABP_CUST_DFNTN_AGRD eq '1' then ABP_CUST_DFNTN_AGRD_FLAG = 'Y';
else ABP_CUST_DFNTN_AGRD_FLAG = 'N';

if ABP_HP_CUST_GOVN ne '' then ABP_HP_CUST_GOVN_FLAG = 'Y';
else ABP_HP_CUST_GOVN_FLAG = 'N';

if ABP_INNVTN_ENGGMNT_REF ne '' then ABP_INNVTN_ENGGMNT_REF_FLAG = 'Y';
else ABP_INNVTN_ENGGMNT_REF_FLAG = 'N';

ABP_MTH_SINCE_LAST_MOD = intck('month', ABP_LAST_MOD_DT, snap_dt);
run;

*ES/EG - led based on abp owner role dated on 12/18/2013;

data arc.abp(compress=yes);
set arc.abp;
*ES/EG led;
OWNER = scan(abp_owner_role, 1);
*if OWNER not in ('ES', 'EG') then call missing(OWNER);

*OneHP;
if acct_id eq '001G000000mkDWf' then ABP_IND_SEG="OneHP";

* status;
if abp_plan_status =: "Submitted" then abp_plan_status = "Submitted/Pending Review";
run;


*region/subregion/country;
data arc.acct (compress=yes);
set arc.acct;

*region;
select ;
when (scan(acct_geo_hierarchy2, 1, ';') eq 'Americas')  acct_subreg1 = 'AMS';
when (scan(acct_geo_hierarchy2, 1, ';') eq 'EMEA')  acct_subreg1 = 'EMEA';
when (scan(acct_geo_hierarchy2, 1, ';') eq 'Asia Pacific') acct_subreg1 = 'APJ';
when (scan(acct_geo_hierarchy2, 1, ';') eq 'SEA') acct_subreg1 = 'APJ';
otherwise;
end;

*subregion;
acct_subreg2 =  scan(acct_geo_hierarchy2, 2, ';') ;
if acct_reg eq 'SEA' then acct_subreg2 = 'AP without Japan';

*country;
acct_subreg3 = scan(strip(acct_geo_hierarchy2), -1, ';') ;
if acct_subreg3 eq 'Japan' then acct_subreg2 = 'Japan';
if acct_subreg3 ne 'Japan' and acct_subreg2 in ('APJeC', 'China') then acct_subreg2 = 'AP without Japan';

*OneHP;
if acct_id eq '001G000000mkDWf' then do;
acct_subreg2 = 'OneHP';
acct_subreg3 = 'OneHP';
end;
run;



*seven practice indicators based on Strategy Initiatives;
data arc.stratinit (compress=yes);
set  arc.stratinit;
format SI_Mobility_Workplace SI_Workload_Cloud SI_Analytics_Data_MGMT 
SI_App_Proj_Servs SI_Enterprise_Security SI_Bus_Process_Servs SI_Industry_Solution $1.;
if find(si_solution, 'Mobility') > 0 
	or find(lowcase(si_nm), 'mobil') > 0 
	or find(lowcase(si_nm), 'workplace') > 0
then SI_Mobility_Workplace = 'Y';
else SI_Mobility_Workplace = 'N';
if find(si_solution, 'Converged Cloud') > 0 
	or find(si_solution, 'IT Performance Suite') > 0 
	or find(si_solution, 'Converged Infrastructure') > 0 
	or find(lowcase(si_nm), 'cloud') > 0 
	or find(lowcase(si_nm), 'workload') > 0
then SI_Workload_Cloud = 'Y';
else SI_Workload_Cloud = 'N';
if find(si_solution, 'Big Data') > 0 
	or find(lowcase(si_nm), 'big data') > 0 
	or find(lowcase(si_nm), 'analytic') > 0 
	or find(lowcase(si_nm), 'data management') > 0 
then SI_Analytics_Data_MGMT = 'Y';
else SI_Analytics_Data_MGMT = 'N';
if find(si_solution, 'Application Transformation') > 0 
	or find(si_solution, 'Information Optimization') > 0 
	or find(lowcase(si_nm), 'transformation') > 0 
	or find(lowcase(si_nm), 'project') > 0 
	or find(lowcase(si_nm), 'application') > 0 
then SI_App_Proj_Servs = 'Y';
else SI_App_Proj_Servs = 'N';
if find(si_solution, 'Security and Risk Management') > 0 
	or find(lowcase(si_nm), 'security') > 0 
	or find(lowcase(si_nm), 'risk') > 0 
then SI_Enterprise_Security = 'Y';
else SI_Enterprise_Security = 'N';
if find(si_solution, 'Managed Print Services') > 0 
	or find(lowcase(si_nm), 'bpo') > 0 
	or find(lowcase(si_nm), 'bps') > 0 
	or find(lowcase(si_nm), 'business process') > 0 
then SI_Bus_Process_Servs = 'Y';
else SI_Bus_Process_Servs = 'N';
if  find(lowcase(si_nm), 'industry') > 0 
then SI_Industry_Solution = 'Y';
else SI_Industry_Solution = 'N';
run;



*Stage 0 opportunity based on the stratinit;
/*
data arc.opp ;
set arc.opp (rename=(SI_PROJ_INIT_VAL_CONV=OPP_TTL_CON_S0));
run;
*/


data work.opp;
set arc.opp;
if opp_sls_stage eq '' then do; 
opp_sls_stage = '00';
end;
OPP_TTL_CON_S0 = 0;
run;

proc sort data= arc.stratinit (keep=si_id snap_dt SI_PROJ_INIT_VAL_CONV)
out=work.tmp (rename=(si_id=hp_si_id SI_PROJ_INIT_VAL_CONV=OPP_TTL_CON_S0)) nodup ;
by si_id snap_dt;
run;

proc sort data= work.opp;
by hp_si_id snap_dt;
run;

data work.final;
merge work.opp (in=a) work.tmp (in=b);
by hp_si_id snap_dt;
if a;
run;

data arc.opp (compress=YES);
set work.final;
if OPP_TTL_CON_S0 eq . then OPP_TTL_CON_S0 = 0;
if opp_sls_stage ne '00' then OPP_TTL_CON_S0 = 0;
run;


**completeness of the fields;
*for si;
data work.stratinit;
set  arc.stratinit;
si_desc_len = lengthn(trim(si_desc)); 
SI_SOLUTION_len = lengthn(trim(SI_SOLUTION));
SI_CUST_BUS_CASE_len = lengthn(trim(SI_CUST_BUS_CASE));
SI_Dependencies_len = lengthn(trim(SI_Dependencies));
SI_RISK_len = lengthn(trim(SI_RISK));
if SI_PROJ_INIT_VAL_CONV ne . then SI_PROJ_INIT_VAL_score = 100;
else SI_PROJ_INIT_VAL_score = 0;
run;


proc sort data=work.stratinit;
by snap_dt;
run;

proc rank data=work.stratinit out=arc.stratinit (compress=yes) groups=100;
by snap_dt;
var si_desc_len SI_SOLUTION_len SI_CUST_BUS_CASE_len SI_Dependencies_len SI_RISK_len;
ranks si_desc_score SI_SOLUTION_score SI_CUST_BUS_CASE_score SI_Dependencies_score SI_RISK_score;
run;


*completeness for bup;
data work.bup;
set  arc.bup;
BUP_NOTES_len = lengthn(trim(BUP_NOTES)); 
BUP_DETL_len = lengthn(trim(BUP_DETL));
BUP_ISSUES_len = lengthn(trim(BUP_ISSUES));
BUP_DETL_STAT_len = lengthn(trim(BUP_DETL_STAT));

if BUP_SOW ne . then BUP_SOW_score = 100;
else BUP_SOW_score = 0;
if BUP_TAM_AMT_CONV ne . then BUP_TAM_AMT_score = 100;
else BUP_TAM_AMT_score = 0;

if BUP_FY11_SLS_AMT_CONV ne . then BUP_FY11_SLS_AMT_score = 100;
else BUP_FY11_SLS_AMT_score = 0;
if BUP_FY12_SLS_AMT_CONV ne . then BUP_FY12_SLS_AMT_score = 100;
else BUP_FY12_SLS_AMT_score = 0;

/***Edited on Feb/06/2015 to match new BUP template 
if BUP_FY13_SLS_AMT_CONV ne . then BUP_FY13_SLS_AMT_score = 100;
else BUP_FY13_SLS_AMT_score = 0;
if BUP_FY14_SLS_AMT_CONV ne . then BUP_FY14_SLS_AMT_score = 100;
else BUP_FY14_SLS_AMT_score = 0;*/
if BUP_FY13_P_SLS_AMT_CONV ne . then BUP_FY13_P_SLS_AMT_score = 100;
else BUP_FY13_P_SLS_AMT_score = 0;
if BUP_FY13_A_SLS_AMT_CONV ne . then BUP_FY13_A_SLS_AMT_score = 100;
else BUP_FY13_A_SLS_AMT_score = 0;
if BUP_FY14_P_SLS_AMT_CONV ne . then BUP_FY14_P_SLS_AMT_score = 100;
else BUP_FY14_P_SLS_AMT_score = 0;
if BUP_FY14_A_SLS_AMT_CONV ne . then BUP_FY14_A_SLS_AMT_score = 100;
else BUP_FY14_A_SLS_AMT_score = 0;

if BUP_FY15_SLS_AMT_CONV ne . then BUP_FY15_SLS_AMT_score = 100;
else BUP_FY15_SLS_AMT_score = 0;
if BUP_FY16_SLS_AMT_CONV ne . then BUP_FY16_SLS_AMT_score = 100;
else BUP_FY16_SLS_AMT_score = 0;
if BUP_FY17_SLS_AMT_CONV ne . then BUP_FY17_SLS_AMT_score = 100;
else BUP_FY17_SLS_AMT_score = 0;

*Added on Feb/06/2015 to match new BUP template;
if BUP_FY18_SLS_AMT_CONV ne . then BUP_FY18_SLS_AMT_score = 100;
else BUP_FY18_SLS_AMT_score = 0;



run;


proc sort data=work.bup;
by snap_dt;
run;

proc rank data=work.bup out=arc.bup (compress=yes) groups=100;
by snap_dt;
var BUP_NOTES_len BUP_DETL_len BUP_ISSUES_len BUP_DETL_STAT_len;
ranks BUP_NOTES_score BUP_DETL_score BUP_ISSUES_score BUP_DETL_STAT_score;
run;



*completeness for bugs;
data work.bus;
set  arc.bus;
BUS_BU_Soltn_len = lengthn(trim(BUS_BU_Soltn)); 
BUS_Challenge_len = lengthn(trim(BUS_Challenge));
BUS_Cust_IT_Prior_len = lengthn(trim(BUS_Cust_IT_Prior_Addrsd));
BUS_Req_Proc_Chg_len = lengthn(trim(BUS_Req_Proc_Change));
BUS_Exectn_Act_len = lengthn(trim(BUS_Exectn_Act));
BUS_HP_Diff_len = lengthn(trim(BUS_HP_Diff));
BUS_Comp_React_len = lengthn(trim(BUS_Comp_React));
BUS_Soltn_Comp_len = lengthn(trim(BUS_Solution_Components));
BUS_Partn_Strat_len = lengthn(trim(BUS_Partn_Inflnce_Strat));
if BUS_TCV_Amt_Conv ne . then BUS_TCV_Amt_score = 100;
else BUS_TCV_Amt_score = 0;
run;


proc sort data=work.bus;
by snap_dt;
run;

proc rank data=work.bus out=arc.bus (compress=yes) groups=100;
by snap_dt;
var BUS_BU_Soltn_len BUS_Challenge_len BUS_Cust_IT_Prior_len BUS_Req_Proc_Chg_len
BUS_Exectn_Act_len BUS_HP_Diff_len BUS_Comp_React_len BUS_Soltn_Comp_len BUS_Partn_Strat_len;
ranks BUS_BU_Soltn_score BUS_Challenge_score BUS_Cust_IT_Prior_score BUS_Req_Proc_Chg_score
BUS_Exectn_Act_score BUS_HP_Diff_score BUS_Comp_React_score BUS_Soltn_Comp_score BUS_Partn_Strat_score;
run;



*completeness for oppty;
data work.opp;
set  arc.opp;
OPP_PRI_CAMP_NM_len = lengthn(trim(OPP_PRI_CAMP_NM)); 
OPP_PRI_CAMP_SRC_len = lengthn(trim(OPP_PRI_CAMP_SOURCE));
OPP_HPSOLUTION_len = lengthn(trim(OPP_HPSOLUTION));
OPP_PRI_PARTN_len = lengthn(trim(OPP__PRI_PARTN_ACCT));
OPP_PRI_COMP_len = lengthn(trim(OPP_PRI_COMP));
if OPP_TTL_CON ne . then OPP_TTL_VAL_score = 100;
else OPP_TTL_VAL_score = 0;
if OPP_PRI_COMP ne '' then OPP_PRI_COMP_score = 100;
else OPP_PRI_COMP_score = 0;
if OPP_FORECAST_CAT ne '' then OPP_FORECAST_score = 100;
else OPP_FORECAST_score = 0;
run;


proc sort data=work.opp;
by snap_dt;
run;

proc rank data=work.opp out=arc.opp (compress=yes) groups=100;
by snap_dt;
var OPP_PRI_CAMP_NM_len OPP_PRI_CAMP_SRC_len OPP_HPSOLUTION_len OPP_PRI_PARTN_len OPP_PRI_COMP_len;
ranks OPP_PRI_CAMP_NM_score OPP_PRI_CAMP_SRC_score OPP_HPSOLUTION_score OPP_PRI_PARTN_score OPP_PRI_COMP_score;
run;


*********************************************************************************************************************************;
*********************************************************************************************************************************;
*********Step 5 validation of the data **********************************************;
*********************************************************************************************************************************;
*********************************************************************************************************************************;

proc sql;
create table work.val01 as 
select *
from arc.abp
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val02 as 
select *
from arc.acct
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val03 as 
select *
from arc.bup
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val04 as 
select *
from arc.compls
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val05 as 
select *
from arc.custprior
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val06 as 
select *
from arc.partnerlandscape
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val07 as 
select *
from arc.opp
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val08 as 
select *
from arc.stratinit
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val09 as 
select *
from arc.bus
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val10 as 
select *
from arc.tce
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val11 as 
select *
from arc.scorecard
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val12 as 
select *
from arc.bugsoppty
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;

proc sql;
create table work.val13 as 
select *
from arc.acctcustomercontact
where acct_plan_id = 'a0iG0000001ZrPM'
order by snap_dt
;
quit;


*********************************************************************************************************************************;
*********************************************************************************************************************************;
**************Step 6 export the data as flat files*********************************;
*********************************************************************************************************************************;
*********************************************************************************************************************************;

*generate the flatfile for QV;
libname ARC  "/var/blade/data2031/esiblade/AcctGrowth/Data/arc";
%let filepath=/var/blade/data2031/esiblade/AcctGrowth/Data/output/;

%let tfilenm1=abp;
%let tfilenm2=acct;
%let tfilenm3=bup;
%let tfilenm4=compls;
%let tfilenm5=custprior;
%let tfilenm6=partnerlandscape;
%let tfilenm7=opp;
%let tfilenm8=stratinit;
%let tfilenm9=bus;
%let tfilenm10=tce;
%let tfilenm11=scorecard;
%let tfilenm13=bugsoppty;
%let tfilenm14=acctcustomercontact;
%let tfilenm15=bup_FY15;


%macro maskfields(fieldlist=);
%let i=1;
%let field = %qscan(&fieldlist,&i, %str( ));
%do %while(&field ne %str( ));
&field = '**********';
%let i=%eval(&i+1);
%let field = %qscan(&fieldlist,&i, %str( ));
%end;
%mend maskfields;

/*
options mprint symbolgen;

data work.tcetest;
set arc.tce;
where snap_dt = '19DEC2013'd;
%maskfields(fieldlist=acct_plan_nm tce_status);
run;
*/

%macro expfile(in=,path=,out=);
*replace existing '|' with '/' before exporting since '|' will be field dilimiter;
data work.exftmp;
set arc.&in.;
array col _CHARACTER_;
do i=1 to dim(col);
col{i}=tranwrd(col{i},'|', '/' );
if col{i} eq '' then col{i} = 'N/A';
end;
drop i;
run;

*mask the private account info;
proc sql;
select distinct "'"||acct_plan_id||"'"  into :PrivateList separated by ','
from arc.acct a
where ACCT_PRIV eq '1';
quit;

data work.&in.;
set work.exftmp;
/***  Once a week for the most recent month, the end on the month for the most recent year, and a full year  ***/;
/***  Added in Jun/26/2014 
 Delete columns for Sep/2013 (and beginning month) data if bup.txt is too big to upload to sharepoint  
'27SEP2013'd,'29NOV2013'd,
****/
if snap_dt in ('27mar2015'd,'30apr2015'd,'29may2015'd,'25jun2015'd,'31jul2015'd,'27aug2015'd,'24sep2015'd,'29oct2015'd,'25nov2015'd,
			   '10dec2015'd,'28jan2016'd,'25feb2016'd,'31mar2016'd,'28apr2016'd,'19may2016'd,'26may2016'd) then do;
*Reduce file size of bup.txt;
BUP_DETL = ' '; BUP_DETL_STAT = ' '; BUP_ISSUES = ' '; BUP_KEY_CONTACT = ' '; BUP_NOTES = ' '; BUP_PPT_ITG_DESC = ' ';
*Reduce file size of abp.txt;
abp_cust_strat = ' '; abp_exec_asks = ' '; abp_hp_cust_state = ' '; abp_key_issue = ' '; abp_strat_opp = ' ';
*Reduce file size of bugsoppty.txt;
OPP_HPSOLUTION = ' '; OPP_NAME = ' '; OPP_NM = ' '; BUP_Plan_NM = ' ';
*Reduce file size of acctcustomercontact.txt;
CRM_TITLE = ' '; CRM_ADDTNL_INFO = ' '; CRM_BUCONTACT = ' ';
*Reduce file size of opp.txt;
opp_pri_chnlpartn_acct = ' '; opp_pri_camp_nm = ' '; opp_pri_camp_source = ' '; opp_pri_comp = ' '; opp_owner = ' '; opp_owner_nm = ' '; 

end;

/*if snap_dt in ('27MAR2014'd,'28AUG2014'd',25SEP2014'd,'27NOV2014'd) then do; */
BUP_DETL = ' '; BUP_DETL_STAT = ' '; CREATOR = ' '; OPP_LAST_MOD_NM = ' '; 
/*end;*/

if snap_dt in ('27SEP2013'd,'25OCT2013'd,'29NOV2013'd,'27DEC2013'd,'30JAN2014'd,'27FEB2014'd,'27MAR2014'd,'24APR2014'd
			  ,'29MAY2014'd,'27NOV2014'd,'18DEC2014'd,'29jan2015'd,'26feb2015'd) then delete;



if acct_plan_id in (&privateList) then do;
%if "&in." = "acct" %then %do;
*call missing(acct_nm, ACCT_LATIN_NM, ACCT_ALT_NM); 
%maskfields(fieldlist =acct_nm ACCT_LATIN_NM ACCT_ALT_NM);
ACCT_PRIV = '1';
%end;
%if "&in." = "abp" %then %do; 
*call missing(acct_plan_nm, ABP_CUST_DFN_INNVTN,ABP_HP_INNVTN_STRAT,ABP_STRAT_OPP,ABP_KEY_ISSUE, ABP_CUST_STRAT, ABP_HP_CUST_STATE);
%maskfields(fieldlist =acct_plan_nm ABP_CUST_DFN_INNVTN ABP_HP_INNVTN_STRAT ABP_STRAT_OPP ABP_KEY_ISSUE ABP_CUST_STRAT ABP_HP_CUST_STATE);
%end;
%if "&in." = "bup" %then %do;
*call missing(acct_plan_nm,BUP_DETL_STAT, BUP_KEY_CONTACT,BUP_ISSUES,BUP_DETL);
%maskfields(fieldlist = acct_plan_nm BUP_DETL_STAT BUP_KEY_CONTACT BUP_ISSUES BUP_DETL);
%end;
%if "&in." = "bus" %then %do;
*call missing(acct_plan_nm, BUS_NM, BUS_Exectn_Act, BUS_Comp_React, BUS_Partn_Inflnce_Strat, BUS_Req_Proc_Change);
%maskfields(fieldlist = acct_plan_nm BUS_NM BUS_Exectn_Act BUS_Comp_React BUS_Partn_Inflnce_Strat BUS_Req_Proc_Change);
%end;
%if "&in." = "compls" %then %do;
*call missing(acct_plan_nm, CL_COMMENTS);
%maskfields(fieldlist = acct_plan_nm CL_COMMENTS);
%end;
%if "&in." = "custprior" %then %do;
*call missing(acct_plan_nm,CBP_NM,CBP_EXP_OUTCOME,CBP_DESC,CBP_DETL_DESC);
%maskfields(fieldlist = acct_plan_nm CBP_NM CBP_EXP_OUTCOME CBP_DESC CBP_DETL_DESC);
%end;
%if "&in." = "opp" %then %do;
*call missing(acct_plan_nm, OPP_NM);
%maskfields(fieldlist = acct_plan_nm OPP_NM);
%end;
%if "&in." = "partnerlandscape" %then %do;
*call missing(acct_plan_nm,PARTN_HP_STRAT, PARTN_FOCUS);
%maskfields(fieldlist = acct_plan_nm PARTN_HP_STRAT PARTN_FOCUS);
%end;
%if "&in." = "scorecard" %then %do;
*call missing(acct_plan_nm);
%maskfields(fieldlist = acct_plan_nm);
%end;
%if "&in." = "stratinit" %then %do;
*call missing(acct_plan_nm, SI_NM, SI_DESC, SI_CUST_BUS_CASE, SI_RISK, SI_Dependencies, SI_CUST_SPONSOR);
%maskfields(fieldlist = acct_plan_nm SI_NM SI_DESC SI_CUST_BUS_CASE SI_RISK SI_Dependencies SI_CUST_SPONSOR);
%end;
%if "&in." = "tce" %then %do;
*call missing(acct_plan_nm);
%maskfields(fieldlist = acct_plan_nm);
%end;
%if "&in." = "bugsoppty" %then %do;
*call missing(acct_plan_nm);
%maskfields(fieldlist = acct_plan_nm BUP_Plan_NM BUS_NM OPP_NM OPP_NAME);
%end;
%if "&in." = "acctcustomercontact" %then %do;
*call missing(acct_plan_nm);
%maskfields(fieldlist = acct_plan_nm);
%end;
end;
where (snap_dt between intnx('month', snap_dt, 0, 'E') - 6 and intnx('month',snap_dt, 0, 'E'))
or snap_dt gt  intnx('day', today(), -23)
or snap_dt = '15may2014'd
or snap_dt = '18dec2014'd
or snap_dt = '10dec2015'd
;
run;

*export with '|' as delimiter;
proc export data=work.&in. outfile="&path.&out..txt" dbms=DLM replace;
delimiter='7c'x;
run;

%mend expfile;

*options symbolgen mprint mlogic;

%expfile(in=&tfilenm1., path=&filepath., out=&tfilenm1.);
%expfile(in=&tfilenm2., path=&filepath., out=&tfilenm2.);
%expfile(in=&tfilenm3., path=&filepath., out=&tfilenm15.);
%expfile(in=&tfilenm4., path=&filepath., out=&tfilenm4.);
%expfile(in=&tfilenm5., path=&filepath., out=&tfilenm5.);
%expfile(in=&tfilenm6., path=&filepath., out=&tfilenm6.);
%expfile(in=&tfilenm7., path=&filepath., out=&tfilenm7.);
%expfile(in=&tfilenm8., path=&filepath., out=&tfilenm8.);
%expfile(in=&tfilenm9., path=&filepath., out=&tfilenm9.);
%expfile(in=&tfilenm10., path=&filepath., out=&tfilenm10.);
%expfile(in=&tfilenm11., path=&filepath., out=&tfilenm11.);
%expfile(in=&tfilenm13., path=&filepath., out=&tfilenm13.);
%expfile(in=&tfilenm14., path=&filepath., out=&tfilenm14.);




/*
check SI opportunity and project values


proc sql;
select snap_dt, ACCT_SUBREG1, ACCT_SUBREG2, count(*) as cnt
from arc.acct
where ACCT_SUBREG1 = 'APJ'
group by 1, 2, 3
order by 1, 2, 3;
quit;


proc sql;
create table work.geo as 
select *
from arc.abp
where acct_plan_id = 'a0iG0000001bRlC'
;
quit;


proc sql;
create table work.test as 
select snap_dt, count(acct_plan_id) as cnt
from arc.stratinit
group by snap_dt
order by snap_dt
;
quit;

***to determine what dates should be kept
proc sql;
select distinct snap_dt
from arc.bup
where (snap_dt between intnx('month', snap_dt, 0, 'E') - 6 and intnx('month',snap_dt, 0, 'E'))
or snap_dt gt  intnx('day', today(), -23)
or snap_dt = '15may2014'd
or snap_dt = '18dec2014'd
or snap_dt = '10dec2015'd
;
quit;


*/
