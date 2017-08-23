
libname UKI "/var/blade/data2031/esiblade/zchunyou/UK_I/Data"; 
libname temp "/var/blade/data2031/esiblade/zchunyou/data";


/*** Consolidate all Finance EDW data into one table **
proc append base=FY11_13 data=FY_14 force;
run;

data temp.REVENUE_11_14;
	set FY11_13;
run;
*/ 

proc sql;
create table Revenue_test as
select Posting_Object_Client_ID as Client_ID
	  ,Posting_Object_Client_Descriptio as Client_Name
	  ,Posting_Object_Service_Line_Desc as ServiceLine_Name
	  ,Direct_Reporting_Accountability as Region
	  ,VP___General_Manager_Accountabil as SubRegion
	  ,Revenue_Recognition_Fiscal_Year as Fiscal_Year
	  ,ES_Historical_Revenue_Recognitio as Fiscal_Qtr
	  ,ES_Historical_Revenue_Recog_0001 as Fiscal_Month
	  ,case when Cost_Stack_Level_8_Name in ('3xxx Expenses','Allocations','Other Income & Expense','Other Non-TCOW Spend','Divisionalization') then 'Other'
	   	    when Cost_Stack_Level_8_Name in ('Cross Charge Expense','Cross Charge Relief') then 'Cross Charge'
	  	    else Cost_Stack_Level_8_Name end as Finance_Type
	  ,sum(ES_Historical_Data_US_Dollar_Amo) as Dollar_Amount
from temp.REVENUE_11_14
group by Posting_Object_Client_ID 
	  	,Posting_Object_Client_Descriptio 
	 	 ,Posting_Object_Service_Line_Desc 
	  	,Direct_Reporting_Accountability 
	  	,VP___General_Manager_Accountabil 
	  	,Revenue_Recognition_Fiscal_Year 
	  	,ES_Historical_Revenue_Recognitio 
	  	,ES_Historical_Revenue_Recog_0001
	  	,Finance_Type;
quit;


proc sql;
create table Revenue_test2 as 
select Client_ID
	  ,Client_Name
	  ,ServiceLine_Name
	  ,Region
	  ,SubRegion
	  ,Fiscal_Year
	  ,Fiscal_Qtr
	  ,Fiscal_Month
	  ,case when Finance_Type='Revenue' then Dollar_Amount 
	  		else 0 end as Rev
	  ,case when Finance_Type='RCOW' then Dollar_Amount 
	  		else 0 end as RCOW
	  ,case when Finance_Type='Pass-Thru' then Dollar_Amount 
	  		else 0 end as Pass_Thru
	  ,case when Finance_Type='Cross Charge' then Dollar_Amount 
	  		else 0 end as Cross_Charge
	  ,case when Finance_Type='CCOW' then Dollar_Amount 
	  		else 0 end as CCOW
	  ,case when Finance_Type='Other' then Dollar_Amount 
	  		else 0 end as Other
from Revenue_test;
quit;


proc sql;
create table Revenue_test3 as 
select Client_ID
	  ,Client_Name
	  ,ServiceLine_Name
	  ,Region
	  ,SubRegion
	  ,Fiscal_Year
	  ,Fiscal_Qtr
	  ,Fiscal_Month
	  ,sum(Rev) as Total_Revenue
	  ,sum(RCOW) as Total_RCOW
	  ,sum(Pass_Thru) as Total_Pass_Thru
	  ,sum(Cross_Charge) as Total_Cross_Charge
	  ,sum(CCOW) as Total_CCOW
	  ,sum(Other) as Total_Other
	  ,sum(Rev)+sum(RCOW)+sum(Pass_Thru)+sum(Cross_Charge)+sum(CCOW)+sum(Other) as Total_EGM
from Revenue_test2
group by Client_ID
	  ,Client_Name
	  ,ServiceLine_Name
	  ,Region
	  ,SubRegion
	  ,Fiscal_Year
	  ,Fiscal_Qtr
	  ,Fiscal_Month;
quit;


/*** Map AMID4 to Finance and LDSM Data ***/
/*
proc sql;
create table efw_amid4 as 
select distinct Client_ID
			   ,BestAMID
from UKI.eFW_to_AMID;
quit;

data UKI.eFW_to_AMID;
	set efw_amid4;
run;
*/

proc sql;
create table UKI.finance_edw as
select 
	   b.BestAMID as AMID4
	  ,a.*
from Revenue_test3 a
left join UKI.eFW_to_AMID b
	on a.Client_ID=b.Client_ID;
quit;


/*** Calculate the Weighted Moving Average of Revenue and EGM ***/

proc sql;
create table Fin_UKI as 
select *
from UKI.finance_edw
where 
/*  and (Total_Revenue<>0 or Total_EGM<>0)*/
       AMID4 in ('BPX210042669'
				,'DHL875643835'
				,'LLD296267206'
				,'RBS214513087'
				,'VOD289936783'
				,'XXX038503546'
				,'XXX210908687'
				,'XXX505005942'
				,'XXXEU9999928'
				,'XXX778557603');

/*
create table serviceline_count as
select ServiceLine_Name
	  ,count(ServiceLine_Name) as count_SL
from Fin_UKI
group by ServiceLine_Name;*/
quit;

/*
data Fin_UKI;
	set Fin_UKI;
	if Total_Revenue=0 and Total_RCOW=0 and Total_Pass_Thru=0 and Total_Cross_Charge=0 and Total_CCOW=0 and Total_Other=0 then delete;
run;
*/

/*Calculate WA Rev and EGM;
proc sort data=Fin_UKI;
by Client_Name ServiceLine_Name;
run;

data WMA_UKI;
	set Fin_UKI;
	by Client_Name ServiceLine_Name;

	if first.ServiceLine_Name=1 and last.ServiceLine_Name=1 then delete;

	if first.ServiceLine_Name=1 then do;
	Count=0; Subtotal_Count=0; Subtotal_Rev=0; Subtotal_EGM=0;
	end;

	Count+1;
	Subtotal_Count + Count;

	Rev_Temp = Total_Revenue*Count;
	EGM_Temp = Total_EGM*Count;
	Subtotal_Rev + Rev_Temp;
	Subtotal_EGM + EGM_Temp;

	WMA_Rev=Subtotal_Rev/Subtotal_Count;
	WMA_EGM=Subtotal_EGM/Subtotal_Count;
run;

proc sql;
select distinct Client_Name from UKI.finance_edw;
quit;
*/
