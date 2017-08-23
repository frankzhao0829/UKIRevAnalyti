

proc sql;
create table GRIP_Metric1 as
select Customer_AMID_4_ID
	  ,Customer_AMID_4_Name
	  ,SignYR
	  ,Sign_Month
	  ,substr(Taxonomy_ServiceLine,6) as Taxonomy_ServiceLine
	  ,case when Status in ('CANCELLED','LOST') then 'DLNA' 
			else Status end as Last_Status
	  ,Deal_Type
	  ,Sales_Motion
	  ,sum(IncludeinWinRate_Num) as WinRate_Num
	  ,sum(IncludeinWinRate_Den) as WinRate_Den
	  ,sum(ES_TCV___M_) as ES_TCV_Total
	  ,sum(ES_FFYR___M_) as ES_FFYR_Total
	  ,sum(ES_EGM___M_) as ES_EGM_Total
	  ,sum(case when IncludeinWinRate_Num=1 then ES_TCV___M_ else 0 end) as TCV_Num
	  ,sum(case when IncludeinWinRate_Den=1 then ES_TCV___M_ else 0 end) as TCV_Den
	  ,avg(Contract_Length) as AVG_Contract_Length
	  ,count(*) as Count_Opportunities
from UKI.grip_uki_data
group by Customer_AMID_4_ID
	  ,Customer_AMID_4_Name
	  ,SignYR
	  ,Sign_Month
	  ,Taxonomy_ServiceLine
	  ,Last_Status
	  ,Deal_Type
	  ,Sales_Motion;
quit;


/*** Plan A: Combine Sales Motion 1 with 4 ***/
data GRIP_Metric2;
	set GRIP_Metric1;

	if Deal_Type in ('AddOn','New') and Sales_Motion=1 then Sales_Motion=4;
		else if Deal_Type = 'Renewal' and Sales_Motion=1 then Sales_Motion=2;
run;

proc sql;
create table GRIP_Metric3 as
select Customer_AMID_4_ID
	  ,Customer_AMID_4_Name
	  ,SignYR
	  ,Sign_Month
	  ,Taxonomy_ServiceLine
	  ,Last_Status
	  ,Deal_Type
	  ,Sales_Motion
	  ,sum(WinRate_Num) as WinRate_Num
	  ,sum(WinRate_Den) as WinRate_Den
	  ,sum(ES_TCV_Total) as ES_TCV_Total
	  ,sum(ES_FFYR_Total) as ES_FFYR_Total
	  ,sum(ES_EGM_Total) as ES_EGM_Total
	  ,sum(TCV_Num) as TCV_Num
	  ,sum(TCV_Den) as TCV_Den
	  ,avg(AVG_Contract_Length) as AVG_Contract_Length
	  ,sum(Count_Opportunities) as Count_Opportunities
from GRIP_Metric2
group by Customer_AMID_4_ID
	  ,Customer_AMID_4_Name
	  ,SignYR
	  ,Sign_Month
	  ,Taxonomy_ServiceLine
	  ,Last_Status
	  ,Deal_Type
	  ,Sales_Motion;
quit;

*Transpose WinRate_Num;
proc transpose data=GRIP_Metric3 out=GRIP_Winratenum prefix=winrate_num;
	by Customer_AMID_4_ID Customer_AMID_4_Name SignYR Sign_Month Taxonomy_ServiceLine;
	id Last_Status Deal_Type Sales_Motion;
	var WinRate_Num;
run;

*Transpose WinRate_Den;
proc transpose data=GRIP_Metric3 out=GRIP_Winrateden prefix=winrate_den;
	by Customer_AMID_4_ID Customer_AMID_4_Name SignYR Sign_Month Taxonomy_ServiceLine;
	id Last_Status Deal_Type Sales_Motion;
	var WinRate_Den;
run;

*Transpose TCV_Num;
proc transpose data=GRIP_Metric3 out=GRIP_TCVNum prefix=TCV_Num;
	by Customer_AMID_4_ID Customer_AMID_4_Name SignYR Sign_Month Taxonomy_ServiceLine;
	id Last_Status Deal_Type Sales_Motion;
	var TCV_Num;
run;

*Transpose TCV_Den;
proc transpose data=GRIP_Metric3 out=GRIP_TCVDen prefix=TCV_Den;
	by Customer_AMID_4_ID Customer_AMID_4_Name SignYR Sign_Month Taxonomy_ServiceLine;
	id Last_Status Deal_Type Sales_Motion;
	var TCV_Den;
run;

*Transpose AVG_Contract_Length;
proc transpose data=GRIP_Metric3 out=GRIP_AVGCLength prefix=AVG_CLength;
	by Customer_AMID_4_ID Customer_AMID_4_Name SignYR Sign_Month Taxonomy_ServiceLine;
	id Last_Status Deal_Type Sales_Motion;
	var AVG_Contract_Length;
run;

*Transpose Count_Opportunities;
proc transpose data=GRIP_Metric3 out=GRIP_countopp prefix=count_opp;
	by Customer_AMID_4_ID Customer_AMID_4_Name SignYR Sign_Month Taxonomy_ServiceLine;
	id Last_Status Deal_Type Sales_Motion;
	var Count_Opportunities;
run;

*Consolidate all the transposed dataset into one data;
data GRIP_TransposeA;
merge GRIP_Winratenum(drop=_name_) GRIP_Winrateden(drop=_name_) GRIP_TCVNum(drop=_name_)
	  GRIP_TCVDen(drop=_name_) GRIP_AVGCLength(drop=_name_) GRIP_countopp(drop=_name_);
by Customer_AMID_4_ID Customer_AMID_4_Name SignYR Sign_Month Taxonomy_ServiceLine;
run;

/*proc contents data=GRIP_TransposeA;run;*/


data GRIP_TransposeA;
set GRIP_TransposeA;

if winrate_numDLNANew4=. then winrate_numDLNANew4=0;
if winrate_numWONRenewal2=. then winrate_numWONRenewal2=0;
if winrate_numWONNew4=. then winrate_numWONNew4=0;
if winrate_numWONAddOn4=. then winrate_numWONAddOn4=0;
if winrate_numDLNARenewal2=. then winrate_numDLNARenewal2=0;
if winrate_numDLNAAddOn4=. then winrate_numDLNAAddOn4=0;
if winrate_numWONAddOn2=. then winrate_numWONAddOn2=0;
if winrate_numDLNAAddOn2=. then winrate_numDLNAAddOn2=0;
if winrate_denDLNANew4=. then winrate_denDLNANew4=0;
if winrate_denWONRenewal2=. then winrate_denWONRenewal2=0;
if winrate_denWONNew4=. then winrate_denWONNew4=0;
if winrate_denWONAddOn4=. then winrate_denWONAddOn4=0;
if winrate_denDLNARenewal2=. then winrate_denDLNARenewal2=0;
if winrate_denDLNAAddOn4=. then winrate_denDLNAAddOn4=0;
if winrate_denWONAddOn2=. then winrate_denWONAddOn2=0;
if winrate_denDLNAAddOn2=. then winrate_denDLNAAddOn2=0;
if TCV_NumDLNANew4=. then TCV_NumDLNANew4=0;
if TCV_NumWONRenewal2=. then TCV_NumWONRenewal2=0;
if TCV_NumWONNew4=. then TCV_NumWONNew4=0;
if TCV_NumWONAddOn4=. then TCV_NumWONAddOn4=0;
if TCV_NumDLNARenewal2=. then TCV_NumDLNARenewal2=0;
if TCV_NumDLNAAddOn4=. then TCV_NumDLNAAddOn4=0;
if TCV_NumWONAddOn2=. then TCV_NumWONAddOn2=0;
if TCV_NumDLNAAddOn2=. then TCV_NumDLNAAddOn2=0;
if TCV_DenDLNANew4=. then TCV_DenDLNANew4=0;
if TCV_DenWONRenewal2=. then TCV_DenWONRenewal2=0;
if TCV_DenWONNew4=. then TCV_DenWONNew4=0;
if TCV_DenWONAddOn4=. then TCV_DenWONAddOn4=0;
if TCV_DenDLNARenewal2=. then TCV_DenDLNARenewal2=0;
if TCV_DenDLNAAddOn4=. then TCV_DenDLNAAddOn4=0;
if TCV_DenWONAddOn2=. then TCV_DenWONAddOn2=0;
if TCV_DenDLNAAddOn2=. then TCV_DenDLNAAddOn2=0;
if AVG_CLengthDLNANew4=. then AVG_CLengthDLNANew4=0;
if AVG_CLengthWONRenewal2=. then AVG_CLengthWONRenewal2=0;
if AVG_CLengthWONNew4=. then AVG_CLengthWONNew4=0;
if AVG_CLengthWONAddOn4=. then AVG_CLengthWONAddOn4=0;
if AVG_CLengthDLNARenewal2=. then AVG_CLengthDLNARenewal2=0;
if AVG_CLengthDLNAAddOn4=. then AVG_CLengthDLNAAddOn4=0;
if AVG_CLengthWONAddOn2=. then AVG_CLengthWONAddOn2=0;
if AVG_CLengthDLNAAddOn2=. then AVG_CLengthDLNAAddOn2=0;
if count_oppDLNANew4=. then count_oppDLNANew4=0;
if count_oppWONRenewal2=. then count_oppWONRenewal2=0;
if count_oppWONNew4=. then count_oppWONNew4=0;
if count_oppWONAddOn4=. then count_oppWONAddOn4=0;
if count_oppDLNARenewal2=. then count_oppDLNARenewal2=0;
if count_oppDLNAAddOn4=. then count_oppDLNAAddOn4=0;
if count_oppWONAddOn2=. then count_oppWONAddOn2=0;
if count_oppDLNAAddOn2=. then count_oppDLNAAddOn2=0;
run;


*Close Rate and Win Rate Calculations;
data GRIP_TransposeA_cal;
	set GRIP_TransposeA;

	if winrate_denWONAddOn2+winrate_denDLNAAddOn2=0 then AddOn_Motion2_CloseRate=0;
		else AddOn_Motion2_CloseRate=winrate_numWONAddOn2/(winrate_denWONAddOn2+winrate_denDLNAAddOn2);
	if winrate_denWONAddOn4+winrate_denDLNAAddOn4=0 then AddOn_Motion4_CloseRate=0;
		else AddOn_Motion4_CloseRate=winrate_numWONAddOn4/(winrate_denWONAddOn4+winrate_denDLNAAddOn4);
	if winrate_denWONNew4+winrate_denDLNANew4=0 then New_Motion4_CloseRate=0;
		else New_Motion4_CloseRate=winrate_numWONNew4/(winrate_denWONNew4+winrate_denDLNANew4);
	if winrate_denWONRenewal2+winrate_denDLNARenewal2=0 then Renewal_Motion2_CloseRate=0;
		else Renewal_Motion2_CloseRate=winrate_numWONRenewal2/(winrate_denWONRenewal2+winrate_denDLNARenewal2);

	if TCV_DenDLNAAddOn2+TCV_DenWONAddOn2=0 then AddOn_Motion2_WinRate=0;
		else AddOn_Motion2_WinRate=TCV_NumWONAddOn2/(TCV_DenDLNAAddOn2+TCV_DenWONAddOn2);
	if TCV_DenDLNAAddOn4+TCV_DenWONAddOn4=0 then AddOn_Motion4_WinRate=0;	
		else AddOn_Motion4_WinRate=TCV_NumWONAddOn4/(TCV_DenDLNAAddOn4+TCV_DenWONAddOn4);
	if TCV_DenDLNANew4+TCV_DenWONNew4=0 then New_Motion4_WinRate=0;	
		else New_Motion4_WinRate=TCV_NumWONNew4/(TCV_DenDLNANew4+TCV_DenWONNew4);
	if TCV_DenDLNARenewal2+TCV_DenWONRenewal2=0 then Renewal_Motion2_WinRate=0;	
		else Renewal_Motion2_WinRate=TCV_NumWONRenewal2/(TCV_DenDLNARenewal2+TCV_DenWONRenewal2);
	
	drop 
winrate_denDLNAAddOn2
winrate_denDLNAAddOn4
winrate_denDLNANew4
winrate_denDLNARenewal2
winrate_denWONAddOn2
winrate_denWONAddOn4
winrate_denWONNew4
winrate_denWONRenewal2
winrate_numDLNAAddOn2
winrate_numDLNAAddOn4
winrate_numDLNANew4
winrate_numDLNARenewal2
winrate_numWONAddOn2
winrate_numWONAddOn4
winrate_numWONNew4
winrate_numWONRenewal2
TCV_DenDLNAAddOn2
TCV_DenDLNAAddOn4
TCV_DenDLNANew4
TCV_DenDLNARenewal2
TCV_DenWONAddOn2
TCV_DenWONAddOn4
TCV_DenWONNew4
TCV_DenWONRenewal2
TCV_NumDLNAAddOn2
TCV_NumDLNAAddOn4
TCV_NumDLNANew4
TCV_NumDLNARenewal2
TCV_NumWONAddOn2
TCV_NumWONAddOn4
TCV_NumWONNew4
TCV_NumWONRenewal2;

run;


*Map GRIP data to Fin Data;
proc sql;
create table fin_ldsm_grip as 
select a.*
	  ,b.AddOn_Motion2_CloseRate
	  ,AddOn_Motion4_CloseRate
	  ,New_Motion4_CloseRate
	  ,Renewal_Motion2_CloseRate
	  ,AddOn_Motion2_WinRate
	  ,AddOn_Motion4_WinRate
	  ,New_Motion4_WinRate
	  ,Renewal_Motion2_WinRate
	  ,b.AVG_CLengthDLNANew4
	  ,b.AVG_CLengthWONRenewal2
	  ,b.AVG_CLengthWONNew4
	  ,b.AVG_CLengthWONAddOn4
	  ,b.AVG_CLengthDLNARenewal2
	  ,b.AVG_CLengthDLNAAddOn4
	  ,b.AVG_CLengthWONAddOn2
	  ,b.AVG_CLengthDLNAAddOn2
	  ,b.count_oppDLNANew4
	  ,b.count_oppWONRenewal2
	  ,b.count_oppWONNew4
	  ,b.count_oppWONAddOn4
	  ,b.count_oppDLNARenewal2
	  ,b.count_oppDLNAAddOn4
	  ,b.count_oppWONAddOn2
	  ,b.count_oppDLNAAddOn2
from fin_amid a				
left join GRIP_TransposeA_cal b
	on a.AMID4 = b.Customer_AMID_4_ID
   and a.Fiscal_Year=b.SignYR
   and a.Month=b.Sign_Month
   and a.ServiceLine_Name=b.Taxonomy_ServiceLine;
quit;

/* proc contents data=fin_ldsm_grip; run; */

data fin_ldsm_grip;
set fin_ldsm_grip;

if AddOn_Motion2_CloseRate=. then AddOn_Motion2_CloseRate=0;
if AddOn_Motion4_CloseRate=. then AddOn_Motion4_CloseRate=0;
if New_Motion4_CloseRate=. then New_Motion4_CloseRate=0;
if Renewal_Motion2_CloseRate=. then Renewal_Motion2_CloseRate=0;
if AddOn_Motion2_WinRate=. then AddOn_Motion2_WinRate=0;
if AddOn_Motion4_WinRate=. then AddOn_Motion4_WinRate=0;
if New_Motion4_WinRate=. then New_Motion4_WinRate=0;
if Renewal_Motion2_WinRate=. then Renewal_Motion2_WinRate=0;
if AVG_CLengthDLNANew4=. then AVG_CLengthDLNANew4=0;
if AVG_CLengthWONRenewal2=. then AVG_CLengthWONRenewal2=0;
if AVG_CLengthWONNew4=. then AVG_CLengthWONNew4=0;
if AVG_CLengthWONAddOn4=. then AVG_CLengthWONAddOn4=0;
if AVG_CLengthDLNARenewal2=. then AVG_CLengthDLNARenewal2=0;
if AVG_CLengthDLNAAddOn4=. then AVG_CLengthDLNAAddOn4=0;
if AVG_CLengthWONAddOn2=. then AVG_CLengthWONAddOn2=0;
if AVG_CLengthDLNAAddOn2=. then AVG_CLengthDLNAAddOn2=0;
if count_oppDLNANew4=. then count_oppDLNANew4=0;
if count_oppWONRenewal2=. then count_oppWONRenewal2=0;
if count_oppWONNew4=. then count_oppWONNew4=0;
if count_oppWONAddOn4=. then count_oppWONAddOn4=0;
if count_oppDLNARenewal2=. then count_oppDLNARenewal2=0;
if count_oppDLNAAddOn4=. then count_oppDLNAAddOn4=0;
if count_oppWONAddOn2=. then count_oppWONAddOn2=0;
if count_oppDLNAAddOn2=. then count_oppDLNAAddOn2=0;

run;



proc sort data=fin_ldsm_grip out=fin_ldsm_grip_WA;
by AMID4 ServiceLine_Name Fiscal_Year Month;
run;


data fin_ldsm_grip_WA2;
	set fin_ldsm_grip_WA;
	by AMID4 ServiceLine_Name;

	if first.AMID4=1 and first.ServiceLine_Name=1 then do;
	sum_count_oppWONAddOn2=0; sum_count_oppDLNAAddOn2=0; sum_count_AddOn2=0;
	sum_count_oppWONAddOn4=0; sum_count_oppDLNAAddOn4=0; sum_count_AddOn4=0; 
	sum_count_oppDLNANew4 =0; sum_count_oppWONNew4 =0; sum_count_New =0;
	sum_count_oppWONRenewal2 =0; sum_count_oppDLNARenewal2 =0; sum_count_Renewal =0;

	sum_AddOn_Motion2_CloseRate=0; sum_AddOn_Motion4_CloseRate=0; sum_New_Motion4_CloseRate=0; sum_Renewal_Motion2_CloseRate=0;
	sum_AddOn_Motion2_WinRate=0; sum_AddOn_Motion4_WinRate=0; sum_New_Motion4_WinRate=0; sum_Renewal_Motion2_WinRate=0;
	end;

/* Close Rate Calculation */
	sum_count_oppWONAddOn2 + count_oppWONAddOn2;
	sum_count_oppDLNAAddOn2 + count_oppDLNAAddOn2;
	sum_count_AddOn2 = sum_count_oppWONAddOn2 + sum_count_oppDLNAAddOn2;
	sum_AddOn_Motion2_CloseRate + AddOn_Motion2_CloseRate;
	if sum_count_AddOn2=0 then WA_AddOn_Motion2_CloseRate=0;
		else WA_AddOn_Motion2_CloseRate = sum_AddOn_Motion2_CloseRate/sum_count_AddOn2;

	sum_count_oppWONAddOn4 + count_oppWONAddOn4;
	sum_count_oppDLNAAddOn4 + count_oppDLNAAddOn4;
	sum_count_AddOn4 = sum_count_oppWONAddOn4 + sum_count_oppDLNAAddOn4;
	sum_AddOn_Motion4_CloseRate + AddOn_Motion4_CloseRate;
	if sum_count_AddOn4=0 then WA_AddOn_Motion4_CloseRate=0;
		else WA_AddOn_Motion4_CloseRate = sum_AddOn_Motion4_CloseRate/sum_count_AddOn4;

	sum_count_oppDLNANew4 + count_oppDLNANew4;
	sum_count_oppWONNew4 + count_oppWONNew4;
	sum_count_New = sum_count_oppDLNANew4 + sum_count_oppWONNew4;
	sum_New_Motion4_CloseRate + New_Motion4_CloseRate;
	if sum_count_New=0 then	WA_New_Motion4_CloseRate =0; 
		else WA_New_Motion4_CloseRate = sum_New_Motion4_CloseRate/sum_count_New;

	sum_count_oppWONRenewal2 + count_oppWONRenewal2;
	sum_count_oppDLNARenewal2 + count_oppDLNARenewal2;
	sum_count_Renewal = sum_count_oppWONRenewal2 + sum_count_oppDLNARenewal2;
	sum_Renewal_Motion2_CloseRate + Renewal_Motion2_CloseRate;
	if sum_count_Renewal=0 then WA_Renewal_Motion2_CloseRate =0;
		else WA_Renewal_Motion2_CloseRate = sum_Renewal_Motion2_CloseRate/sum_count_Renewal;

/* Win Rate Calculation */
	sum_AddOn_Motion2_WinRate + AddOn_Motion2_WinRate;
	if sum_count_AddOn2=0 then WA_AddOn_Motion2_WinRate =0;
		else WA_AddOn_Motion2_WinRate= sum_AddOn_Motion2_WinRate/sum_count_AddOn2;

	sum_AddOn_Motion4_WinRate + AddOn_Motion4_WinRate;
	if sum_count_AddOn4=0 then WA_AddOn_Motion4_WinRate=0; 
		else WA_AddOn_Motion4_WinRate=sum_AddOn_Motion4_WinRate/sum_count_AddOn4;

	sum_New_Motion4_WinRate + New_Motion4_WinRate;
	if sum_count_New=0 then WA_New_Motion4_WinRate=0; 
		else WA_New_Motion4_WinRate=sum_New_Motion4_WinRate/sum_count_New;

	sum_Renewal_Motion2_WinRate + Renewal_Motion2_WinRate;
	if sum_count_Renewal=0 then WA_Renewal_Motion2_WinRate=0; 
		else WA_Renewal_Motion2_WinRate=sum_Renewal_Motion2_WinRate/sum_count_Renewal;

	drop sum_AddOn_Motion2_CloseRate sum_count_oppWONAddOn2 sum_count_oppDLNAAddOn2
		 sum_count_oppWONAddOn4 sum_count_oppDLNAAddOn4 sum_count_AddOn4 
		 sum_count_oppDLNANew4 sum_count_oppWONNew4 sum_count_New
		 sum_count_oppWONRenewal2 sum_count_oppDLNARenewal2 sum_count_Renewal sum_count_AddOn2
		 sum_AddOn_Motion2_CloseRate sum_AddOn_Motion4_CloseRate sum_New_Motion4_CloseRate sum_Renewal_Motion2_CloseRate
		 sum_AddOn_Motion2_WinRate sum_AddOn_Motion4_WinRate sum_New_Motion4_WinRate sum_Renewal_Motion2_WinRate;
run;

