
libname UKI "/var/blade/data2031/esiblade/zchunyou/UK_I/Data"; 



/* Convert Client ID to Character variable to match Finance Data
data uki.efw_to_amid;
	retain char_client_ID;
	set uki.efw_to_amid;
	char_client_ID = compress(put(Posting_Object_Client_ID___eFW,$4.));

	drop Posting_Object_Client_ID___eFW;
	rename char_client_ID=Posting_Object_Client_ID___eFW;
run;*/


/*** Map AE Score to Finance Data ***/
data AE_Score;
	set UKI.AE_SCORE;
	
	if substr(YrMo,6,2) in ('11','12') then Fiscal_Year = input(substr(YrMo,1,4),4.) + 1;
		else Fiscal_Year = input(substr(YrMo,1,4),4.);
	if substr(YrMo,6,2) = '11' then Fiscal_Month = 1;
		else if substr(YrMo,6,2) = '12' then Fiscal_Month = 2;
		else if substr(YrMo,6,2) = '10' then Fiscal_Month = 12;
		else Fiscal_Month = input(substr(YrMo,7,1),1.) + 2;
run;

proc sql;
create table fin_ldsm_ae as
select a.*
	  ,b.No_of_Clients as AE_No_of_Clients
	  ,b.Norm_Score as AE_Score
from Fin_LDSM_test2 a
left join AE_Score b
	on a.Fiscal_Year=b.Fiscal_Year
   and a.Month=b.Fiscal_Month
   and a.AMID4=b.AMID_4;
quit;


* Change the date to calendar date;
data fin_ldsm_ae;
retain AMID4 Calendar_Year Fiscal_Qtr Calendar_Month;
	set fin_ldsm_ae;
	
	if Month in ('1','2') then Calendar_Year = Fiscal_Year - 1;
		else Calendar_Year = Fiscal_Year;
	if Month = '1' then Calendar_Month = 11;
		else if Month = '2' then Calendar_Month = 12;
		else Calendar_Month = Month - 2;
run;




/*
proc sort data=fin_ldsm_ae;
	by Client_ID ServiceLine_Name Fiscal_Year Month;
run;


data fin_ldsm_ae_test;
	set fin_ldsm_ae;
	retain AE_test;
	if AE_Score ne ' ' then AE_test=AE_Score;
	if AE_test = ' ' then AE_test='86.67%';

	AE_test_num=input(AE_test,5.2);
run;


proc glm data = fin_ldsm_ae_test;
class Client_Name ServiceLine_Name;
model Sum_Revenue_mi Sum_EGM_mi = Client_Name ServiceLine_Name WA_Pct_On_Time_Demand 
						    WA_Pct_On_Time_Fulfillment WA_Avg_Days_To_Fill AE_test_num/ solution ss3;
manova h= _ALL_ ;
run;
*/

