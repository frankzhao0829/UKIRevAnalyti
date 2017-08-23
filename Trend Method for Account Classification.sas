libname UKI "/var/blade/data2031/esiblade/zchunyou/UK_I/Data"; 

proc sql;
create table count_AMID4 as
select AMID4
      ,count(*)
from UKI.finance_edw_account
group by AMID4;
quit;

proc sql;
create table finance_edw_account_48 as
select a.*
	  ,b._TEMG001 as count_months
from UKI.finance_edw_account a
left join count_AMID4 b
on a.AMID4=b.AMID4;
quit;

data finance_edw_account_48 (drop=count_months);
	set finance_edw_account_48;

	if AMID4=' ' then delete;
	if count_months=48;
	Qtr=tranwrd(Fiscal_Qtr,"-","_");
run;

proc sort data=finance_edw_account_48;
	by AMID4 Fiscal_Year Month;
run;

proc sql;
create table account_quarter as
select AMID4
	  ,Qtr 
	  ,sum(Sum_Revenue) as Sum_Rev
	  ,sum(Sum_EGM) as Sum_EGM
from finance_edw_account_48
group by AMID4
	    ,Qtr
order by AMID4
	    ,Qtr;
quit;

proc standard data=account_quarter mean=0 std=1 out=standardized_account;
	by AMID4;
	var Sum_EGM Sum_Rev;
run;

data standardized_account;
	set standardized_account;

	Rev_EGM=mean(Sum_EGM,Sum_Rev);

	if Qtr='2011_Q1' then Qtr_Num=1;
		else if Qtr='2011_Q2' then Qtr_Num=2;
		else if Qtr='2011_Q3' then Qtr_Num=3;
		else if Qtr='2011_Q4' then Qtr_Num=4;
		else if Qtr='2012_Q1' then Qtr_Num=5;
		else if Qtr='2012_Q2' then Qtr_Num=6;
		else if Qtr='2012_Q3' then Qtr_Num=7;
		else if Qtr='2012_Q4' then Qtr_Num=8;
		else if Qtr='2013_Q1' then Qtr_Num=9;
		else if Qtr='2013_Q2' then Qtr_Num=10;
		else if Qtr='2013_Q3' then Qtr_Num=11;
		else if Qtr='2013_Q4' then Qtr_Num=12;
		else if Qtr='2014_Q1' then Qtr_Num=13;
		else if Qtr='2014_Q2' then Qtr_Num=14;
		else if Qtr='2014_Q3' then Qtr_Num=15;
		else Qtr_Num=16;
run;

/*
proc sort data=standardized_account;
	by Qtr AMID4;
run;

proc transpose data=standardized_account out=account_wide prefix=Rev_EGM_;
	by Qtr;
	id amid4;
	var Rev_EGM;
run;

proc varclus data=account_wide maxclusters=3 OUTSTAT=account_wide_clusters;
	var Rev_EGM_3MX006173082--Rev_EGM_XXXWYSLCORST;
run;

data cluster_output;
	set account_wide_clusters;
	if _NCL_=3 and _TYPE_='GROUP';
run;

proc append base=account_wide data=cluster_output force;
run;
*/

/* Untransposed data
proc fastclus data=standardized_account_AMID maxclusters=3 out=temp;
	var Rev_EGM Qtr_Num;
run;
*/

proc sort data=standardized_account out=standardized_account_AMID;
	by AMID4 Qtr_Num;
run;


data standardized_account_AMID1;
   set standardized_account_AMID;
   by AMID4;
   retain HoldRevenue HoldEGM HoldRev_EGM;
   if first.AMID4 then do;
		HoldRevenue = 0;
   	 	HoldEGM = 0;
		HoldRev_EGM = 0;
   end;
   output;
   HoldRevenue = Sum_Rev;
   HoldEGM = Sum_EGM;
   HoldRev_EGM = Rev_EGM;
run;

data standardized_account_AMID2;
   set standardized_account_AMID1;


	if HoldRevenue=0 or Qtr_Num=1 then Rev_Change=0;
		else Rev_Change=(Sum_Rev - HoldRevenue)/HoldRevenue;

	if HoldEGM=0 or Qtr_Num=1 then EGM_Change=0;
		else EGM_Change=(Sum_EGM - HoldEGM)/HoldEGM;

	if HoldRev_EGM=0 or Qtr_Num=1 then Rev_EGM_Change=0;
		else Rev_EGM_Change=(Rev_EGM - HoldRev_EGM)/HoldRev_EGM;

	W_Rev_EGM=Rev_EGM*Qtr_Num;
	if Qtr_Num ne 1;
run;

data standardized_account_AMID3;
    set standardized_account_AMID2;

    by AMID4;

	if first.AMID4=1 then do;
		Total_Qtr=0; WT_Rev_EGM=0;
	end;

	Total_Qtr + Qtr_Num;
	WT_Rev_EGM + W_Rev_EGM;

	if Total_Qtr=0 then Rev_EGM_Change=0;
		else Rev_EGM_Change=WT_Rev_EGM/Total_Qtr;
run;


data standardized_account_AMID4;
    set standardized_account_AMID3;

	if Qtr_Num=16;
	keep amid4 Rev_EGM_Change;
run;


proc fastclus data=standardized_account_AMID4 maxclusters=5 out=temp;
	var Rev_EGM_Change;
run;


/*
data account_quarter_temp;
   set account_quarter;
   by AMID4;
   retain HoldRevenue HoldEGM;
   if first.AMID4 then do;
		HoldRevenue = 0;
   	 	HoldEGM = 0;
   end;
   output;
   HoldRevenue = Sum_Rev;
   HoldEGM = Sum_EGM;
run;


data account_quarter2;
   set account_quarter_temp;

   if Fiscal_Qtr = '2011-Q1' then Order=1;
 	  else if Fiscal_Qtr = '2011-Q2' then Order=2;
	  else if Fiscal_Qtr = '2011-Q3' then Order=3;
	  else if Fiscal_Qtr = '2011-Q4' then Order=4;
	  else if Fiscal_Qtr = '2012-Q1' then Order=5;
	  else if Fiscal_Qtr = '2012-Q2' then Order=6;
	  else if Fiscal_Qtr = '2012-Q3' then Order=7;
	  else if Fiscal_Qtr = '2012-Q4' then Order=8;
	  else if Fiscal_Qtr = '2013-Q1' then Order=9;
	  else if Fiscal_Qtr = '2013-Q2' then Order=10;
	  else if Fiscal_Qtr = '2013-Q3' then Order=11;
	  else if Fiscal_Qtr = '2013-Q4' then Order=12;
	  else if Fiscal_Qtr = '2014-Q1' then Order=13;
	  else if Fiscal_Qtr = '2014-Q2' then Order=14;
	  else if Fiscal_Qtr = '2014-Q3' then Order=15;
	  else Order=16;

	if HoldRevenue=0 or Order=1 then Rev_Change=0;
		else Rev_Change=(Sum_Rev - HoldRevenue)/HoldRevenue;

	if HoldEGM=0 or Order=1 then EGM_Change=0;
		else EGM_Change=(Sum_EGM - HoldEGM)/HoldEGM;

	if Sum_Rev=0 then EGM_Rev=0;
		else EGM_Rev=Sum_EGM/Sum_Rev;
run;


data account_quarter3 (keep=AMID4 Rev_Trend EGM_Trend EGM_Rev_Trend Avg_Trend Label);
   set account_quarter2;

   by AMID4;

   W_Rev=Order*Rev_Change;
   W_EGM=Order*EGM_Change;
   W_EGM_Rev=Order*EGM_Rev;

   if first.AMID4 then do;
		Accu_Order=0; WA_Rev=0; WA_EGM=0; WA_EGM_Rev=0;
   end;
   Accu_Order + Order;
   WA_Rev + W_Rev;
   WA_EGM + W_EGM;
   WA_EGM_Rev + W_EGM_Rev;

   if last.AMID4 then do;
   Rev_Trend=WA_Rev/Accu_Order;
   EGM_Trend=WA_EGM/Accu_Order;
   EGM_Rev_Trend=WA_EGM_Rev/Accu_Order;
   Avg_Trend=mean(Rev_Trend,EGM_Trend,EGM_Rev_Trend);
   end;

   if Avg_Trend >= 0 then Label='Good';
   	  else if Avg_Trend < 0 then Label='Bad';
   if last.AMID4 then output;
run;


proc sql;
create table label_account as
select b.Label
	  ,a.*
from finance_edw_account_48 a
left join account_quarter3 b
on a.AMID4=b.AMID4;
quit;



data work.acct(keep=acct_plan_id acct_id acct_amid acct_nm amid_new);
set ARC.ACCT;
where snap_dt = '27SEP2013'd;
amid_new = compress(acct_amid, '1234567890', 'k');
run;
*/

proc transpose data=work.account_quarter out=account_rev_qtr prefix=Rev_;
	by AMID4;
	id Qtr;
	var Sum_Rev;
run;

proc transpose data=work.account_quarter out=account_egm_qtr prefix=EGM_;
	by AMID4;
	id Qtr;
	var Sum_EGM;
run;

/*
data account_rows;
    merge  account_rev_qtr(drop=_name_) account_egm_qtr(drop=_name_);
    by AMID4;
run;
*/
