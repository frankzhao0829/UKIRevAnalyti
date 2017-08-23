libname UKI "/var/blade/data2031/esiblade/zchunyou/UK_I/Data"; 


data Norm_temp;
	set UKI.All_consolidated;

	sum_RCOW = sum_RCOW * -1;
	Sum_Pass_Thru = Sum_Pass_Thru * -1;
	Sum_Cross_Charge = Sum_Cross_Charge * -1;
	Sum_CCOW = Sum_CCOW * -1;
	Sum_Other = Sum_Other * -1;
run;

proc sql;
create table Norm_prep as
select AMID4
	  ,min(sum_RCOW) as min_RCOW
	  ,max(sum_RCOW) as max_RCOW
	  ,min(Sum_Pass_Thru) as min_Pass_Thru
	  ,max(Sum_Pass_Thru) as max_Pass_Thru
	  ,min(Sum_Cross_Charge) as min_Cross_Charge
	  ,max(Sum_Cross_Charge) as max_Cross_Charge
	  ,min(Sum_CCOW) as min_CCOW
	  ,max(Sum_CCOW) as max_CCOW
	  ,min(Sum_Other) as min_Other
	  ,max(Sum_Other) as max_Other
from Norm_temp
group by AMID4;
quit;

data UKI_temp;
	set Norm_temp;
run;

proc sql;
create table UKI_Normalized as
select a.*
	  ,b.min_RCOW
	  ,b.max_RCOW
	  ,b.min_Pass_Thru
	  ,b.max_Pass_Thru
	  ,b.min_Cross_Charge
	  ,b.max_Cross_Charge
	  ,b.min_CCOW
	  ,b.max_CCOW
	  ,b.min_Other
	  ,b.max_Other
from UKI_temp a
left join Norm_prep b
on a.AMID4=b.AMID4;
quit;


data UKI_Normalized2;
	set UKI_Normalized;

	RCOW_Norm = (sum_RCOW - min_RCOW)/(max_RCOW - min_RCOW);
	Pass_Thru_Norm = (Sum_Pass_Thru - min_Pass_Thru)/(max_Pass_Thru - min_Pass_Thru);
	Cross_Charge_Norm = (Sum_Cross_Charge - min_Cross_Charge)/(max_Cross_Charge - min_Cross_Charge);
	CCOW_Norm = (Sum_CCOW - min_CCOW)/(max_CCOW - min_CCOW);
	Other_Norm = (Sum_Other - min_Other)/(max_Other - min_Other);

	 drop min_RCOW 
		  max_RCOW
		  min_Pass_Thru
		  max_Pass_Thru
		  min_Cross_Charge
		  max_Cross_Charge
		  min_CCOW
		  max_CCOW
		  min_Other
		  max_Other;
run;


data UKI_PCA;
	set UKI_Normalized2;
run;

proc contents data=UKI_PCA; run;


/* GRIP Index */
proc factor data=UKI_PCA outstat=FA_GRIP msa cov score;
	var AVG_CLengthWONRenewal2
		AVG_CLengthWONNew4
		AVG_CLengthWONAddOn4
		AVG_CLengthWONAddOn2
		count_oppWONRenewal2
		count_oppWONNew4
		count_oppWONAddOn4
		count_oppDLNARenewal2
		count_oppDLNAAddOn4
		count_oppWONAddOn2
		count_oppDLNAAddOn2
		WA_AddOn_Motion2_CloseRate
		WA_AddOn_Motion4_CloseRate
		WA_New_Motion4_CloseRate
		WA_Renewal_Motion2_CloseRate
		WA_AddOn_Motion2_WinRate
		WA_AddOn_Motion4_WinRate
		WA_New_Motion4_WinRate
		WA_Renewal_Motion2_WinRate;
run;

/* CM Index */
proc factor data=UKI_PCA outstat=FA_CM msa cov score;
	var CM_Successful
		CM_UnSuccessful_Changes_which_ar
		CM_UnSuccessful_Changes_which_do
		CM_UnSuccessful_changes_with_unk
		CM___APPS
		CM___Cross_Functional___Multiple
		CM___DC_Facilities_Mgmt
		CM___EAO
		CM___ECSO
		CM___GSD
		CM___HPIT
		CM___ITO_OTHER
		CM___Mainframe
		CM___Networking_Services
		CM___Security_Services
		CM___Software_Services
		CM___Storage__Backup
		CM___UNIX_LINUX
		CM___Wintel
		/*CM___Workplace_Services*/
		CM___Emergency
		CM___NORMAL
		CM___Routine
		CM___Standard
		/*CM___NA*/;
run;

/* Rtop Index */
proc factor data=UKI_PCA outstat=FA_Rtop msa cov score;
	var RtOP_Count___ALPHA
		RtOP_Count___iRtOP
		RtOP_Count___RtOP
		RtOP_Count___vRtOP
		RtOP_CDT___ALPHA
		RtOP_CDT___iRtOP
		RtOP_CDT___RtOP
		RtOP_CDT___vRtOP
		RtOP_WDT___ALPHA
		RtOP_WDT___iRtOP
		RtOP_WDT___RtOP
		RtOP_WDT___vRtOP
		/*RtOP_ADT___ALPHA*/
		RtOP_ADT___iRtOP
		RtOP_ADT___RtOP
		RtOP_ADT___vRtOP
		/*RtOP___Significant*/
		RtOP___High
		RtOP___Medium
		RtOP___APPS_Org_
		RtOP___BPO_Org_
		RtOP___Business_Continuity_Recov
		RtOP___Client___Client_3rd_Party
		RtOP___Enterprise_Applications_O
		RtOP___Enterprise_Service_Mgmt__
		RtOP___HPIT
		RtOP___HPTS
		RtOP___Mainframe
		RtOP___Midrange___Database___Mid
		RtOP___Midrange___Legacy_Platfor
		RtOP___Midrange___UNIX_LINUX
		RtOP___Midrange___Wintel
		RtOP___Network
		RtOP___Other___Please_Specify
		RtOP___Production_Engineering
		RtOP___Program___Project_Managem
		RtOP___Security
		RtOP___Software_Services
		RtOP___Storage_Backup
		RtOP___Utility_Services_Cloud
		/*RtOP___WorkPlace_Services*/;
run;

/* IM Index */
proc factor data=UKI_PCA outstat=FA_IM msa cov score;
	var IM___Critical_CaseCount
		IM___High_CaseCount
		IM___Others_CaseCount
		IM___Critical_TTRMetNotCaseCount
		IM___High_TTRMetNotCaseCount
		IM___Others_TTRMetNotCaseCount
		IM___APPS
		IM___Cross_Functional___Multiple
		IM___EAO
		/*IM___ECSO*/
		IM___ESM
		IM___GSD
		IM___HPIT
		IM___ITO_OTHER
		IM___Mainframe
		IM___Networking_Services
		IM___Security_Services
		IM___Software_Services
		IM___Storage__Backup
		IM___UNIX_LINUX
		IM___Wintel
		IM___WorkPlace_Services;
run;

/* ABS Index */
proc factor data=UKI_PCA outstat=FA_ABS msa cov score;
	var ABS_TOTAL_COUNT
		ABS_UTILIZATION_RATE
		ABS_SINGLEACCOUNT_UTILIZATION_RA
		ABS_HIGH_COST_COUNTRY_COUNT
		ABS_HIGH_COST_COUNTRY_UTILIZATIO
		ABS_LOW_COST_COUNTRY_COUNT
		ABS_LOW_COST_COUNTRY_UTILIZATION
		ABS_GLOBAL_CENTER_COUNT
		ABS_GLOBAL_CENTER_UTILIZATION
		ABS_LOCAL_DELIVERY_COUNT
		ABS_LOCAL_DELIVERY_UTILIZATION
		ABS_REGIONAL_CENTER_COUNT
		/*ABS_REGIONAL_CENTER_UTILIZATION
		ABS_VOLUMETRIC_SERVICE_COUNT
		ABS_VOLUMETRIC_SERVICE_UTILIZATI
		ABS_Internal_COUNT
		ABS_INTERNAL_UTILIZATION
		ABS_VOLUMETRIC_DEMAND_COUNT
		ABS_VOLUMETRIC_DEMAND_UTILIZATIO
		ABS_ACCOUNT_FUNDED_FTE_COUNT
		ABS_ACCOUNT_FUNDED_FTE_UTILIZATI
		ABS_Overhead_COUNT
		ABS_Overhead_UTILIZATION*/;
run;

/* SD Index */
proc factor data=UKI_PCA outstat=FA_SD msa cov score;
	var ttir_avg_database_management
		ttir_avg_enterprise_applications
		ttir_avg_network_management
		ttir_avg_server_management
		ttir_avg_storage_management
		ttir_avg_customer_care
		ttir_avg_global_service_desk
		/*ttir_avg_managed_mainframe*/
		ttir_avg_managed_messaging
		ttir_avg_enterprise_cloud_servic
		ttir_avg_customer_specific
		ttr_avg_database_management
		ttr_avg_enterprise_applications
		ttr_avg_network_management
		ttr_avg_server_management
		ttr_avg_storage_management
		ttr_avg_customer_care
		ttr_avg_global_service_desk
		/*ttr_avg_managed_mainframe*/
		ttr_avg_managed_messaging
		ttr_avg_enterprise_cloud_service
		ttr_avg_customer_specific
		ct_inci_server_management
		ct_inci_enterprise_cloud_service
		ct_inci_database_management
		ct_inci_global_service_desk
		ct_inci_customer_care
		ct_inci_network_management
		ct_inci_enterprise_applications
		ct_inci_managed_messaging
		ct_inci_storage_management
		/*ct_inci_managed_mainframe*/
		ct_inci_customer_specific
		ctir_avg_customer_care
		ctir_avg_database_management
		ctir_avg_enterprise_applications
		ctir_avg_global_service_desk
		ctir_avg_managed_messaging
		ctir_avg_network_management
		ctir_avg_server_management
		ctir_avg_service_catalog
		ctir_avg_storage_management
		ct_inter_server_management
		ct_inter_global_service_desk
		/*ct_inter_service_catalog*/
		ct_inter_managed_messaging
		ct_inter_network_management
		ct_inter_enterprise_applications
		/*ct_inter_storage_management*/
		ct_inter_customer_care
		/*ct_inter_database_management*/;
run;


proc score data=UKI_PCA score=FA_GRIP out=UKI_GRIP_scores ;
run;

proc score data=UKI_PCA score=FA_CM out=UKI_CM_scores ;
run;

proc score data=UKI_PCA score=FA_Rtop out=UKI_Rtop_scores ;
run;

proc score data=UKI_PCA score=FA_IM out=UKI_IM_scores ;
run;

proc score data=UKI_PCA score=FA_ABS out=UKI_ABS_scores ;
run;

proc score data=UKI_PCA score=FA_SD out=UKI_SD_scores ;
run;


proc sql;
create table UKI_PCA_Consolidated as
select a.WA_AddOn_Motion2_CloseRate
	  ,a.WA_AddOn_Motion4_CloseRate
	  ,a.WA_New_Motion4_CloseRate
	  ,a.WA_Renewal_Motion2_CloseRate
	  ,a.WA_AddOn_Motion2_WinRate
	  ,a.WA_AddOn_Motion4_WinRate
	  ,a.WA_New_Motion4_WinRate
	  ,a.WA_Renewal_Motion2_WinRate
	  ,a.AMID4
      ,a.RCOW_Norm 
	  ,a.Cross_Charge_Norm 
	  ,a.CCOW_Norm  
	  ,a.Sum_EGM_Norm
	  ,a.Sum_Revenue_Norm
	  ,a.Factor1 as AVG_CLengthWONAddOn2
	  ,a.Factor2 as AVG_CLengthWONAddOn4
	  ,a.Factor3 as AVG_CLengthWONNew4
	  ,a.Factor4 as Cl_WONRe_w_Ct_WonAdd2
	  ,a.Factor5 as Cl_WONRe_ex_Ct_WonAdd2
	  ,b.Factor1 as CM_Successful_w_Normal
	  ,b.Factor2 as CM_Factor2
	  ,b.Factor3 as CM_NOR_ex_CrossFunctional
	  ,c.Factor1 as RtOP_WDT_iRtOP
	  ,c.Factor2 as RtOP_WDT_RtOP
	  ,d.Factor1 as IM_Others_CaseCount
	  ,d.Factor2 as IM_Factor2
	  ,e.Factor1 as ABS_TOTAL_COUNT
	  ,e.Factor2 as ABS_LC_COUNTRY_GC_COUNT
	  ,f.Factor1 as ttr_avg_customer_specific
	  ,f.Factor2 as ttr_avg_customer_care
	  ,f.Factor3 as ttr_avg_network_management
	  ,f.Factor4 as ttr_avg_serverm_ex_netm
from UKI_GRIP_scores a
left join UKI_CM_scores b 
	 on a.AMID4=b.AMID4
	and a.Fiscal_Year=b.Fiscal_Year
	and a.Month=b.Month
left join UKI_Rtop_scores c
	 on a.AMID4=c.AMID4
	and a.Fiscal_Year=c.Fiscal_Year
	and a.Month=c.Month
left join UKI_IM_scores d
	 on a.AMID4=d.AMID4
	and a.Fiscal_Year=d.Fiscal_Year
	and a.Month=d.Month
left join UKI_ABS_scores e
	 on a.AMID4=e.AMID4
	and a.Fiscal_Year=e.Fiscal_Year
	and a.Month=e.Month
left join UKI_SD_scores f
	 on a.AMID4=f.AMID4
	and a.Fiscal_Year=f.Fiscal_Year
	and a.Month=f.Month;
quit;





proc glm data = UKI_PCA_Consolidated;
by AMID4;
model RCOW_Norm Cross_Charge_Norm CCOW_Norm Sum_EGM_Norm Sum_Revenue_Norm =
	  WA_AddOn_Motion2_CloseRate
	  WA_AddOn_Motion4_CloseRate
	  WA_New_Motion4_CloseRate
	  WA_Renewal_Motion2_CloseRate
      AVG_CLengthWONAddOn2
	  AVG_CLengthWONAddOn4
	  AVG_CLengthWONNew4
	  Cl_WONRe_w_Ct_WonAdd2
	  Cl_WONRe_ex_Ct_WonAdd2
	  CM_Successful_w_Normal
	  CM_Factor2
	  CM_NOR_ex_CrossFunctional
	  RtOP_WDT_iRtOP
	  RtOP_WDT_RtOP
	  IM_Others_CaseCount
	  IM_Factor2
	  ABS_TOTAL_COUNT
	  ABS_LC_COUNTRY_GC_COUNT
	  ttr_avg_customer_specific
	  ttr_avg_customer_care
	  ttr_avg_network_management
	  ttr_avg_serverm_ex_netm/ solution ss3;
run;
