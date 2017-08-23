
libname UKI "/var/blade/data2031/esiblade/zchunyou/UK_I/Data";
libname temp "/var/blade/data2031/esiblade/zchunyou/data";

/* Aggregate Incidents data 
proc append base=Incidents_13 data=Incidents_14 force;
run;

proc append base=Incidents_BP_CUH data=Incidents_13 force;
run;

data temp.Incidents_ALL;
	set Incidents_BP_CUH;
run;
*/

data UKI.Incidents_All;
	set temp.Incidents_ALL;
	
	if CO_NM = 'AON' then AMID4 = 'XXX038503546';
		else if CO_NM = 'AVIV' then AMID4 = 'XXX505005942';
		else if CO_NM = 'BPWW' then AMID4 = 'BPX210042669';
		else if CO_NM = 'CENT' then AMID4 = 'XXX778557603';
		else if CO_NM = 'DHL' then AMID4 = 'DHL875643835';
		else if CO_NM = 'LTSR' then AMID4 = 'LLD296267206';
		else if CO_NM = 'RBSR' then AMID4 = 'RBS214513087';
		else if CO_NM = 'RYCE' then AMID4 = 'XXX210908687';
		else if CO_NM = 'VODA' then AMID4 = 'VOD289936783';
		else delete;

	Year = year(CLOSE_CLDR_DT);
	Month = month(CLOSE_CLDR_DT);

	if SRVC_LN_NM = 'enterprise applications o' then SRVC_LN_NM = 'enterprise_applications';
		else if SRVC_LN_NM = 'managed messaging & colla' then SRVC_LN_NM = 'managed_messaging';
		else if SRVC_LN_NM = 'customer care' then SRVC_LN_NM = 'customer_care';
		else if SRVC_LN_NM = 'customer specific' then SRVC_LN_NM = 'customer_specific';
		else if SRVC_LN_NM = 'database management' then SRVC_LN_NM = 'database_management';
		else if SRVC_LN_NM = 'enterprise cloud services' then SRVC_LN_NM = 'enterprise_cloud_services';
		else if SRVC_LN_NM = 'global service desk' then SRVC_LN_NM = 'global_service_desk';
		else if SRVC_LN_NM = 'managed mainframe' then SRVC_LN_NM = 'managed_mainframe';
		else if SRVC_LN_NM = 'network management' then SRVC_LN_NM = 'network_management';
		else if SRVC_LN_NM = 'server management' then SRVC_LN_NM = 'server_management';
		else if SRVC_LN_NM = 'storage management' then SRVC_LN_NM = 'storage_management';

	if TTIR_DUR_SEC_QT=. then TTIR_DUR_SEC_QT=0;
	if SLO_TTR_DUR_SEC_QT=. then SLO_TTR_DUR_SEC_QT=0;
run;


proc sql;
create table Incidents_Aggre as 
select AMID4
	  ,Year 
	  ,Month
	  ,SRVC_LN_NM
	  ,avg(TTIR_DUR_SEC_QT) as avg_TTIR
	  ,avg(SLO_TTR_DUR_SEC_QT) as avg_TTR
	  ,count(*) as count_Incidents
from UKI.Incidents_All
group by AMID4 
	    ,Year 
	    ,Month
	    ,SRVC_LN_NM
order by AMID4 
	    ,Year 
	    ,Month
	    ,SRVC_LN_NM;
quit;


proc transpose data=Incidents_Aggre out=Incidents_ttir prefix=ttir_avg_;
	by AMID4 Year Month;
	id SRVC_LN_NM;
	var avg_TTIR;
run;

proc transpose data=Incidents_Aggre out=Incidents_ttr prefix=ttr_avg_;
	by AMID4 Year Month;
	id SRVC_LN_NM;
	var avg_TTR;
run;

proc transpose data=Incidents_Aggre out=Incidents_ct prefix=ct_inci_;
	by AMID4 Year Month;
	id SRVC_LN_NM;
	var count_Incidents;
run;


data Incidents_Transpose;
merge Incidents_ttir(drop=_name_) Incidents_ttr(drop=_name_) Incidents_ct(drop=_name_);
by AMID4 Year Month;
run;


*Map Incidents to Fin data;
proc sql;
create table fin_ldsm_grip_abs_ito_incidents as 
select a.*
      ,b.ttir_avg_database_management
      ,b.ttir_avg_enterprise_applications
      ,b.ttir_avg_network_management
      ,b.ttir_avg_server_management
      ,b.ttir_avg_storage_management
      ,b.ttir_avg_customer_care
      ,b.ttir_avg_global_service_desk
      ,b.ttir_avg_managed_mainframe
      ,b.ttir_avg_managed_messaging
      ,b.ttir_avg_enterprise_cloud_servic
      ,b.ttir_avg_customer_specific
      ,b.ttr_avg_database_management
      ,b.ttr_avg_enterprise_applications
      ,b.ttr_avg_network_management
      ,b.ttr_avg_server_management
      ,b.ttr_avg_storage_management
      ,b.ttr_avg_customer_care
      ,b.ttr_avg_global_service_desk
      ,b.ttr_avg_managed_mainframe
      ,b.ttr_avg_managed_messaging
      ,b.ttr_avg_enterprise_cloud_service
      ,b.ttr_avg_customer_specific
	  ,b.ct_inci_server_management
	  ,b.ct_inci_enterprise_cloud_service
	  ,b.ct_inci_database_management
	  ,b.ct_inci_global_service_desk
	  ,b.ct_inci_customer_care
	  ,b.ct_inci_network_management
	  ,b.ct_inci_enterprise_applications
	  ,b.ct_inci_managed_messaging
	  ,b.ct_inci_storage_management
	  ,b.ct_inci_managed_mainframe
	  ,b.ct_inci_customer_specific
from fin_ldsm_grip_abs_ito a
left join Incidents_Transpose b
	on a.AMID4=b.AMID4
   and a.Calendar_Year=b.Year
   and a.Calendar_Month=b.Month;
quit;

/*
data fin_ldsm_grip_abs_ito_incidents;
	set fin_ldsm_grip_abs_ito_incidents;

if ttir_avg_database_management=. then ttir_avg_database_management=0;
if ttir_avg_enterprise_applications=. then ttir_avg_enterprise_applications=0;
if ttir_avg_network_management=. then ttir_avg_network_management=0;
if ttir_avg_server_management=. then ttir_avg_server_management=0;
if ttir_avg_storage_management=. then ttir_avg_storage_management=0;
if ttir_avg_customer_care=. then ttir_avg_customer_care=0;
if ttir_avg_global_service_desk=. then ttir_avg_global_service_desk=0;
if ttir_avg_managed_mainframe=. then ttir_avg_managed_mainframe=0;
if ttir_avg_managed_messaging=. then ttir_avg_managed_messaging=0;
if ttir_avg_enterprise_cloud_servic=. then ttir_avg_enterprise_cloud_servic=0;
if ttir_avg_customer_specific=. then ttir_avg_customer_specific=0;
if ttr_avg_database_management=. then ttr_avg_database_management=0;
if ttr_avg_enterprise_applications=. then ttr_avg_enterprise_applications=0;
if ttr_avg_network_management=. then ttr_avg_network_management=0;
if ttr_avg_server_management=. then ttr_avg_server_management=0;
if ttr_avg_storage_management=. then ttr_avg_storage_management=0;
if ttr_avg_customer_care=. then ttr_avg_customer_care=0;
if ttr_avg_global_service_desk=. then ttr_avg_global_service_desk=0;
if ttr_avg_managed_mainframe=. then ttr_avg_managed_mainframe=0;
if ttr_avg_managed_messaging=. then ttr_avg_managed_messaging=0;
if ttr_avg_enterprise_cloud_service=. then ttr_avg_enterprise_cloud_service=0;
if ttr_avg_customer_specific=. then ttr_avg_customer_specific=0;
run;
*/

/* Aggregate Interactions data */
data UKI.Interactions_All;
	set temp.Interaction_Closed;
	
	if CO_NM = 'AON' then AMID4 = 'XXX038503546';
		else if CO_NM = 'AVIV' then AMID4 = 'XXX505005942';
		else if CO_NM = 'BPWW' then AMID4 = 'BPX210042669';
		else if CO_NM = 'CENT' then AMID4 = 'XXX778557603';
		else if CO_NM = 'DHL' then AMID4 = 'DHL875643835';
		else if CO_NM = 'LTSR' then AMID4 = 'LLD296267206';
		else if CO_NM = 'RBSR' then AMID4 = 'RBS214513087';
		else if CO_NM = 'RYCE' then AMID4 = 'XXX210908687';
		else if CO_NM = 'VODA' then AMID4 = 'VOD289936783';
		else delete;

	Year = year(CLOSE_CLDR_DT);
	Month = month(CLOSE_CLDR_DT);

	if SRVC_LN_NM = 'enterprise applications operations' then SRVC_LN_NM = 'enterprise_applications';
		else if SRVC_LN_NM = 'managed messaging & collaboration' then SRVC_LN_NM = 'managed_messaging';
		else if SRVC_LN_NM = 'customer care' then SRVC_LN_NM = 'customer_care';
		else if SRVC_LN_NM = 'customer specific' then SRVC_LN_NM = 'customer_specific';
		else if SRVC_LN_NM = 'database management' then SRVC_LN_NM = 'database_management';
		else if SRVC_LN_NM = 'enterprise cloud services' then SRVC_LN_NM = 'enterprise_cloud_services';
		else if SRVC_LN_NM = 'global service desk' then SRVC_LN_NM = 'global_service_desk';
		else if SRVC_LN_NM = 'managed mainframe' then SRVC_LN_NM = 'managed_mainframe';
		else if SRVC_LN_NM = 'network management' then SRVC_LN_NM = 'network_management';
		else if SRVC_LN_NM = 'server management' then SRVC_LN_NM = 'server_management';
		else if SRVC_LN_NM = 'storage management' then SRVC_LN_NM = 'storage_management';
		else if SRVC_LN_NM = 'service catalog' then SRVC_LN_NM = 'service_catalog';
		else delete;

	if CTIR_DUR_SEC_QT=. then CTIR_DUR_SEC_QT=0;
run;


proc sql;
create table Interactions_Aggre as 
select AMID4 
	  ,Year 
	  ,Month
	  ,SRVC_LN_NM
	  ,avg(CTIR_DUR_SEC_QT) as avg_CTIR
	  ,count(*) as count_Interactions
from UKI.Interactions_All
group by AMID4 
	    ,Year 
	    ,Month
	    ,SRVC_LN_NM
order by AMID4 
	    ,Year 
	    ,Month
	    ,SRVC_LN_NM;
quit;


proc transpose data=Interactions_Aggre out=Interactions_ctir prefix=ctir_avg_;
	by AMID4 Year Month;
	id SRVC_LN_NM;
	var avg_CTIR;
run;

proc transpose data=Interactions_Aggre out=Interactions_ct prefix=ct_inter_;
	by AMID4 Year Month;
	id SRVC_LN_NM;
	var count_Interactions;
run;

data Interactions_Transpose;
merge Interactions_ctir(drop=_name_) Interactions_ct(drop=_name_);
by AMID4 Year Month;
run;

/*proc contents data=Interactions_Transpose; run;*/


*Map Interactions to Fin data;
proc sql;
create table all_consolidated_temp as 
select a.*
      ,b.ctir_avg_customer_care
      ,b.ctir_avg_database_management
      ,b.ctir_avg_enterprise_applications
      ,b.ctir_avg_global_service_desk
      ,b.ctir_avg_managed_messaging
      ,b.ctir_avg_network_management
      ,b.ctir_avg_server_management
      ,b.ctir_avg_service_catalog
      ,b.ctir_avg_storage_management
	  ,b.ct_inter_server_management
	  ,b.ct_inter_global_service_desk
	  ,b.ct_inter_service_catalog
	  ,b.ct_inter_managed_messaging
	  ,b.ct_inter_network_management
	  ,b.ct_inter_enterprise_applications
	  ,b.ct_inter_storage_management
	  ,b.ct_inter_customer_care
	  ,b.ct_inter_database_management
from fin_ldsm_grip_abs_ito_incidents a
left join Interactions_Transpose b
	on a.AMID4=b.AMID4
   and a.Calendar_Year=b.Year
   and a.Calendar_Month=b.Month;
quit;

data UKI.all_consolidated;
	set all_consolidated_temp;
run;

/*
data UKI.all_consolidated;
	set all_consolidated_temp;

if ctir_avg_customer_care=. then ctir_avg_customer_care=0;
if ctir_avg_database_management=. then ctir_avg_database_management=0;
if ctir_avg_enterprise_applications=. then ctir_avg_enterprise_applications=0;
if ctir_avg_global_service_desk=. then ctir_avg_global_service_desk=0;
if ctir_avg_managed_messaging=. then ctir_avg_managed_messaging=0;
if ctir_avg_network_management=. then ctir_avg_network_management=0;
if ctir_avg_server_management=. then ctir_avg_server_management=0;
if ctir_avg_service_catalog=. then ctir_avg_service_catalog=0;
if ctir_avg_storage_management=. then ctir_avg_storage_management=0;
run;
*/

