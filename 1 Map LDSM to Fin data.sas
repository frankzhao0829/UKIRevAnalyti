libname UKI "/var/blade/data2031/esiblade/zchunyou/UK_I/Data"; 



*OTD Data;
proc sql;
create table LDSM_OTD_AMID as
select 
	   b.BestAMID as AMID4
	  ,a.*
from uki.ldsm_otd a
left join UKI.eFW_to_AMID b
	on a.Client_ID=b.Client_ID;

*OTD Data;
create table LDSM_OTF_AMID as
select 
	   b.BestAMID as AMID4
	  ,a.*
from UKI.ldsm_otf a
left join UKI.eFW_to_AMID b
	on a.Client_ID=b.Client_ID;

*TTF Data;

create table LDSM_TTF_AMID as
select 
	   b.BestAMID as AMID4
	  ,a.*
from uki.ldsm_ttf a
left join UKI.eFW_to_AMID b
	on a.Client_ID=b.Client_ID;
quit;


/* Combine and delete service lines in Fin Data to match LDSM  */
data Fin_UKI_AMID;
	set Fin_UKI_AMID;

	if ServiceLine_Name in ('DO NOT USE Enterprise Services OCOS','ES COMPASS CONTRACT COST CENTER (RCC)','ES HQ') then delete;
	if ServiceLine_Name in ('BLANK','NO VALUE') then ServiceLine_Name='Other';
	if ServiceLine_Name in ('BPS Overhead') then ServiceLine_Name='BPO Overhead';
run;


proc sql;
create table Fin_UKI2 as 
select AMID4
/*	  ,ServiceLine_Name*/
	  ,input(SUBSTR(Fiscal_Year,3,4),4.) as Fiscal_Year
	  ,Fiscal_Qtr
	  ,case when Fiscal_Month in('FY2011-10','FY2012-10','FY2013-10','FY2014-10') then 10
	  	    when Fiscal_Month in('FY2011-11','FY2012-11','FY2013-11','FY2014-11') then 11
			when Fiscal_Month in('FY2011-12','FY2012-12','FY2013-12','FY2014-12') then 12
	  		else input(SUBSTR(Fiscal_Month,9,1),2.) end as Month
	  ,sum(Total_Revenue) as Sum_Revenue
	  ,sum(Total_RCOW) as Sum_RCOW
	  ,sum(Total_Pass_Thru) as Sum_Pass_Thru
	  ,sum(Total_Cross_Charge) as Sum_Cross_Charge
	  ,sum(Total_CCOW) as Sum_CCOW
	  ,sum(Total_Other) as Sum_Other
	  ,sum(Total_EGM) as Sum_EGM
from Fin_UKI
group by AMID4
/*	  ,ServiceLine_Name*/
	  ,Fiscal_Year
	  ,Fiscal_Qtr
	  ,Fiscal_Month;
quit;

proc contents data=uki.ldsm_otd;
proc contents data=uki.ldsm_otf;
proc contents data=uki.ldsm_ttf;
run;

/* Aggregate LDSM data based on Service_Line Level */
proc sql;
create table ldsm_otd as
select 
 AMID4
/*,Portfolio
,Service_Line*/
,ROD_Approval_Year
,ROD_Approval_Month
,sum(Positions_Requested) as Positions_Requested
,sum(FTE_Requested) as FTE_Requested
,avg(Pct_On_Time_Demand) as Pct_On_Time_Demand
,sum(Avg_Days_Notice_Provide) as Avg_Days_Notice_Provide
from LDSM_OTD_AMID
group by
 AMID4
/*,Portfolio*/
,ROD_Approval_Year
,ROD_Approval_Month
/*,Service_Line*/;

create table ldsm_otf as
select 
 AMID4
/*,Portfolio
,Service_Line*/
,Position_Start_Year
,Position_Start_Month
,sum(Positions_Starts) as Positions_Starts
,avg(On_Time_Fulfillment) as On_Time_Fulfillment
,sum(Pct_On_Time_Fulfillment) as Pct_On_Time_Fulfillment
from LDSM_OTF_AMID
group by 
 AMID4
/*,Portfolio*/
,Position_Start_Year
,Position_Start_Month
/*,Service_Line*/;

create table ldsm_ttf as
select 
 AMID4
/*,Portfolio
,Service_Line*/
,Position_Fill_Year
,Position_Fill_Month
,sum(Positions_Filled) as Positions_Filled
,sum(Days_Of_Fulfilment) as Days_Of_Fulfilment
,avg(Avg_Days_To_Fill) as Avg_Days_To_Fill
from LDSM_TTF_AMID
group by 
 AMID4
/*,Portfolio*/
,Position_Fill_Year
,Position_Fill_Month
/*,Service_Line*/;
quit;


/*** Map LDSM data to Fin_EDW data ***/

proc sql;
/* Map LDSM_OTD to Fin Data  */
create table Fin_UKI3 as
select a.*
	  ,b.Positions_Requested
	  ,b.FTE_Requested
	  ,b.Pct_On_Time_Demand
	  ,b.Avg_Days_Notice_Provide
from Fin_UKI2 a
left join ldsm_otd b
on a.AMID4=b.AMID4
/*and a.ServiceLine_Name=b.Service_Line*/
and a.Fiscal_Year=b.ROD_Approval_Year
and a.Month=b.ROD_Approval_Month;

/* Map LDSM_OTF to Fin Data  */
create table Fin_UKI4 as
select a.*
	  ,b.Positions_Starts
	  ,b.On_Time_Fulfillment
	  ,b.Pct_On_Time_Fulfillment
from Fin_UKI3 a
left join ldsm_otf b
on a.AMID4=b.AMID4
/*and a.ServiceLine_Name=b.Service_Line*/
and a.Fiscal_Year=b.Position_Start_Year
and a.Month=b.Position_Start_Month;

/* Map LDSM_TTF to Fin Data  */
create table UKI.Fin_LDSM as
select a.*
	  ,b.Positions_Filled
	  ,b.Days_Of_Fulfilment
	  ,b.Avg_Days_To_Fill
from Fin_UKI4 a
left join ldsm_ttf b
on a.AMID4=b.AMID4
/*and a.ServiceLine_Name=b.Service_Line*/
and a.Fiscal_Year=b.Position_Fill_Year
and a.Month=b.Position_Fill_Month;
quit;

/*
proc contents data=Fin_UKI5;
run;
*/

/*
data UKI.Fin_LDSM;
	set Fin_UKI5;
	if Positions_Requested = . then Positions_Requested = 0;
	if FTE_Requested = . then FTE_Requested = 0;
	if Pct_On_Time_Demand = . then Pct_On_Time_Demand = 0;
	if Avg_Days_Notice_Provide = . then Avg_Days_Notice_Provide = 0;
	if Positions_Starts = . then Positions_Starts = 0;
	if On_Time_Fulfillment = . then On_Time_Fulfillment = 0;
	if Pct_On_Time_Fulfillment = . then Pct_On_Time_Fulfillment = 0;
	if Positions_Filled = . then Positions_Filled = 0;
	if Days_Of_Fulfilment = . then Days_Of_Fulfilment = 0;
	if Avg_Days_To_Fill = . then Avg_Days_To_Fill = 0;
 run;
*/


/*** Test the significance of OTD, OTF, TTF ***/
proc glm data = UKI.Fin_LDSM;
class Client_Name ServiceLine_Name;
model Sum_Revenue_mi Sum_EGM_mi = Client_Name ServiceLine_Name Pct_On_Time_Demand 
							Pct_On_Time_Fulfillment Avg_Days_To_Fill / solution ss3;
manova h= _ALL_ ;
run;


/*** Test the significance of Weighted Average OTD, OTF, TTF ***/
proc sort data=UKI.Fin_LDSM out=Fin_LDSM_test;
/*by Client_Name ServiceLine_Name Month;*/
by AMID4 Fiscal_Year Month;
run;


data Fin_LDSM_test2;
	set Fin_LDSM_test;
	by AMID4;

	if first.AMID4=1 then do;
	sum_Positions_Requested=0; sum_Pct_On_Time_Demand=0;
	sum_Positions_Starts=0; sum_Pct_On_Time_Fulfillment=0;
	sum_Positions_Filled=0; sum_Avg_Days_To_Fill=0;
	end;

/* OTD Calculation */
	sum_Positions_Requested + Positions_Requested;
	sum_Pct_On_Time_Demand + Pct_On_Time_Demand;
	if sum_Positions_Requested=0 then WA_Pct_On_Time_Demand=0;
		else WA_Pct_On_Time_Demand=sum_Pct_On_Time_Demand/sum_Positions_Requested;

/* OTF Calculation */
	sum_Positions_Starts + Positions_Starts;
	sum_Pct_On_Time_Fulfillment + Pct_On_Time_Fulfillment;
	if sum_Positions_Starts=0 then WA_Pct_On_Time_Fulfillment=0;
		else WA_Pct_On_Time_Fulfillment=sum_Pct_On_Time_Fulfillment/sum_Positions_Starts;

/* TTF Calculation */
	sum_Positions_Filled + Positions_Filled;
	sum_Avg_Days_To_Fill + Avg_Days_To_Fill;
	if sum_Positions_Filled=0 then WA_Avg_Days_To_Fill=0;
		else WA_Avg_Days_To_Fill=sum_Avg_Days_To_Fill/sum_Positions_Filled;

	drop 
FTE_Requested
Avg_Days_Notice_Provide
On_Time_Fulfillment
Days_Of_Fulfilment
sum_Positions_Requested
sum_Pct_On_Time_Demand
sum_Positions_Starts
sum_Pct_On_Time_Fulfillment
sum_Positions_Filled
sum_Avg_Days_To_Fill;

run;




/*
proc glm data = Fin_LDSM_test2;
class Client_Name ServiceLine_Name;
model Sum_Revenue_mi Sum_EGM_mi = Client_Name ServiceLine_Name WA_Pct_On_Time_Demand 
						    WA_Pct_On_Time_Fulfillment WA_Avg_Days_To_Fill / solution ss3;
manova h= _ALL_ ;
run;
*/

