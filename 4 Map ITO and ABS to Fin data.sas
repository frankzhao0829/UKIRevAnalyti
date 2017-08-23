
libname UKI "/var/blade/data2031/esiblade/zchunyou/UK_I/Data"; 

/*proc contents data=UKI.uki_ito;run;*/
data uki_ito;
retain Year Month;
	set UKI.uki_ito;
	Month=input(substr(YrMo,6,2),2.);
	Year=input(substr(YrMo,1,4),4.);
run;


*Map ITO data to Fin Data;
proc sql;
create table fin_ldsm_grip_ito_temp as 
select a.*
      ,b.CM_Successful
      ,b.CM_UnSuccessful_Changes_which_ar
      ,b.CM_UnSuccessful_Changes_which_do
      ,b.CM_UnSuccessful_changes_with_unk
      ,b.CM___Total_Changes
      ,b.CM___APPS
      ,b.CM___Cross_Functional___Multiple
      ,b.CM___DC_Facilities_Mgmt
      ,b.CM___EAO
      ,b.CM___ECSO
      ,b.CM___GSD
      ,b.CM___HPIT
      ,b.CM___ITO_OTHER
      ,b.CM___Mainframe
      ,b.CM___Networking_Services
      ,b.CM___Security_Services
      ,b.CM___Software_Services
      ,b.CM___Storage__Backup
      ,b.CM___UNIX_LINUX
      ,b.CM___Wintel
      ,b.CM___Workplace_Services
      ,b.CM___Emergency
      ,b.CM___NORMAL
      ,b.CM___Routine
      ,b.CM___Standard
      ,b.CM___NA
      ,b.RtOP_Count___ALPHA
      ,b.RtOP_Count___iRtOP
      ,b.RtOP_Count___RtOP
      ,b.RtOP_Count___vRtOP
      ,b.RtOP_CDT___ALPHA
      ,b.RtOP_CDT___iRtOP
      ,b.RtOP_CDT___RtOP
      ,b.RtOP_CDT___vRtOP
      ,b.RtOP_WDT___ALPHA
      ,b.RtOP_WDT___iRtOP
      ,b.RtOP_WDT___RtOP
      ,b.RtOP_WDT___vRtOP
      ,b.RtOP_ADT___ALPHA
      ,b.RtOP_ADT___iRtOP
      ,b.RtOP_ADT___RtOP
      ,b.RtOP_ADT___vRtOP
      ,b.RtOP___Significant
      ,b.RtOP___High
      ,b.RtOP___Medium
      ,b.RtOP___APPS_Org_
      ,b.RtOP___BPO_Org_
      ,b.RtOP___Business_Continuity_Recov
      ,b.RtOP___Client___Client_3rd_Party
      ,b.RtOP___Enterprise_Applications_O
      ,b.RtOP___Enterprise_Service_Mgmt__
      ,b.RtOP___HPIT
      ,b.RtOP___HPTS
      ,b.RtOP___Mainframe
      ,b.RtOP___Midrange___Database___Mid
      ,b.RtOP___Midrange___Legacy_Platfor
      ,b.RtOP___Midrange___UNIX_LINUX
      ,b.RtOP___Midrange___Wintel
      ,b.RtOP___Network
      ,b.RtOP___Other___Please_Specify
      ,b.RtOP___Production_Engineering
      ,b.RtOP___Program___Project_Managem
      ,b.RtOP___Security
      ,b.RtOP___Software_Services
      ,b.RtOP___Storage_Backup
      ,b.RtOP___Utility_Services_Cloud
      ,b.RtOP___WorkPlace_Services
      ,b.IM___Critical_CaseCount
      ,b.IM___High_CaseCount
      ,b.IM___Others_CaseCount
      ,b.IM___Critical_TTRMetNotCaseCount
      ,b.IM___High_TTRMetNotCaseCount
      ,b.IM___Others_TTRMetNotCaseCount
      ,b.IM___APPS
      ,b.IM___Cross_Functional___Multiple
      ,b.IM___EAO
      ,b.IM___ECSO
      ,b.IM___ESM
      ,b.IM___GSD
      ,b.IM___HPIT
      ,b.IM___ITO_OTHER
      ,b.IM___Mainframe
      ,b.IM___Networking_Services
      ,b.IM___Security_Services
      ,b.IM___Software_Services
      ,b.IM___Storage__Backup
      ,b.IM___UNIX_LINUX
      ,b.IM___Wintel
      ,b.IM___WorkPlace_Services
from UKI.fin_ldsm_grip a
left join uki_ito b
	on a.AMID4=b.AMID_4
   and a.Calendar_Year=b.Year
   and a.Calendar_Month=b.Month;
quit;


*Map ABS data to Fin Data;
proc sql;
create table fin_ldsm_grip_abs_ito as 
select a.*
      ,b.ABS_TOTAL_COUNT
      ,b.ABS_UTILIZATION_RATE
      ,b.ABS_SINGLEACCOUNT_UTILIZATION_RA
      ,b.ABS_HIGH_COST_COUNTRY_COUNT
      ,b.ABS_HIGH_COST_COUNTRY_UTILIZATIO
      ,b.ABS_LOW_COST_COUNTRY_COUNT
      ,b.ABS_LOW_COST_COUNTRY_UTILIZATION
      ,b.ABS_GLOBAL_CENTER_COUNT
      ,b.ABS_GLOBAL_CENTER_UTILIZATION
      ,b.ABS_LOCAL_DELIVERY_COUNT
      ,b.ABS_LOCAL_DELIVERY_UTILIZATION
      ,b.ABS_REGIONAL_CENTER_COUNT
      ,b.ABS_REGIONAL_CENTER_UTILIZATION
      ,b.ABS_VOLUMETRIC_SERVICE_COUNT
      ,b.ABS_VOLUMETRIC_SERVICE_UTILIZATI
      ,b.ABS_Internal_COUNT
      ,b.ABS_INTERNAL_UTILIZATION
      ,b.ABS_VOLUMETRIC_DEMAND_COUNT
      ,b.ABS_VOLUMETRIC_DEMAND_UTILIZATIO
      ,b.ABS_ACCOUNT_FUNDED_FTE_COUNT
      ,b.ABS_ACCOUNT_FUNDED_FTE_UTILIZATI
      ,b.ABS_Overhead_COUNT
      ,b.ABS_Overhead_UTILIZATION
from fin_ldsm_grip_ito_temp a
left join UKI.ABS_SUMMARY b
	on a.AMID4=b.CLIENTAMIDLEVEL4ID
   and a.Calendar_Year=b.Year
   and a.Calendar_Month=b.Month;
quit;

/*
data UKI.fin_ldsm_grip_abs_ito;
	set fin_ldsm_grip_abs_ito;

if CM_Successful=. then CM_Successful=0;
if CM_UnSuccessful_Changes_which_ar=. then CM_UnSuccessful_Changes_which_ar=0;
if CM_UnSuccessful_Changes_which_do=. then CM_UnSuccessful_Changes_which_do=0;
if CM_UnSuccessful_changes_with_unk=. then CM_UnSuccessful_changes_with_unk=0;
if CM___Total_Changes=. then CM___Total_Changes=0;
if CM___APPS=. then CM___APPS=0;
if CM___Cross_Functional___Multiple=. then CM___Cross_Functional___Multiple=0;
if CM___DC_Facilities_Mgmt=. then CM___DC_Facilities_Mgmt=0;
if CM___EAO=. then CM___EAO=0;
if CM___ECSO=. then CM___ECSO=0;
if CM___GSD=. then CM___GSD=0;
if CM___HPIT=. then CM___HPIT=0;
if CM___ITO_OTHER=. then CM___ITO_OTHER=0;
if CM___Mainframe=. then CM___Mainframe=0;
if CM___Networking_Services=. then CM___Networking_Services=0;
if CM___Security_Services=. then CM___Security_Services=0;
if CM___Software_Services=. then CM___Software_Services=0;
if CM___Storage__Backup=. then CM___Storage__Backup=0;
if CM___UNIX_LINUX=. then CM___UNIX_LINUX=0;
if CM___Wintel=. then CM___Wintel=0;
if CM___Workplace_Services=. then CM___Workplace_Services=0;
if CM___Emergency=. then CM___Emergency=0;
if CM___NORMAL=. then CM___NORMAL=0;
if CM___Routine=. then CM___Routine=0;
if CM___Standard=. then CM___Standard=0;
if CM___NA=. then CM___NA=0;
if RtOP_Count___ALPHA=. then RtOP_Count___ALPHA=0;
if RtOP_Count___iRtOP=. then RtOP_Count___iRtOP=0;
if RtOP_Count___RtOP=. then RtOP_Count___RtOP=0;
if RtOP_Count___vRtOP=. then RtOP_Count___vRtOP=0;
if RtOP_CDT___ALPHA=. then RtOP_CDT___ALPHA=0;
if RtOP_CDT___iRtOP=. then RtOP_CDT___iRtOP=0;
if RtOP_CDT___RtOP=. then RtOP_CDT___RtOP=0;
if RtOP_CDT___vRtOP=. then RtOP_CDT___vRtOP=0;
if RtOP_WDT___ALPHA=. then RtOP_WDT___ALPHA=0;
if RtOP_WDT___iRtOP=. then RtOP_WDT___iRtOP=0;
if RtOP_WDT___RtOP=. then RtOP_WDT___RtOP=0;
if RtOP_WDT___vRtOP=. then RtOP_WDT___vRtOP=0;
if RtOP_ADT___ALPHA=. then RtOP_ADT___ALPHA=0;
if RtOP_ADT___iRtOP=. then RtOP_ADT___iRtOP=0;
if RtOP_ADT___RtOP=. then RtOP_ADT___RtOP=0;
if RtOP_ADT___vRtOP=. then RtOP_ADT___vRtOP=0;
if RtOP___Significant=. then RtOP___Significant=0;
if RtOP___High=. then RtOP___High=0;
if RtOP___Medium=. then RtOP___Medium=0;
if RtOP___APPS_Org_=. then RtOP___APPS_Org_=0;
if RtOP___BPO_Org_=. then RtOP___BPO_Org_=0;
if RtOP___Business_Continuity_Recov=. then RtOP___Business_Continuity_Recov=0;
if RtOP___Client___Client_3rd_Party=. then RtOP___Client___Client_3rd_Party=0;
if RtOP___Enterprise_Applications_O=. then RtOP___Enterprise_Applications_O=0;
if RtOP___Enterprise_Service_Mgmt__=. then RtOP___Enterprise_Service_Mgmt__=0;
if RtOP___HPIT=. then RtOP___HPIT=0;
if RtOP___HPTS=. then RtOP___HPTS=0;
if RtOP___Mainframe=. then RtOP___Mainframe=0;
if RtOP___Midrange___Database___Mid=. then RtOP___Midrange___Database___Mid=0;
if RtOP___Midrange___Legacy_Platfor=. then RtOP___Midrange___Legacy_Platfor=0;
if RtOP___Midrange___UNIX_LINUX=. then RtOP___Midrange___UNIX_LINUX=0;
if RtOP___Midrange___Wintel=. then RtOP___Midrange___Wintel=0;
if RtOP___Network=. then RtOP___Network=0;
if RtOP___Other___Please_Specify=. then RtOP___Other___Please_Specify=0;
if RtOP___Production_Engineering=. then RtOP___Production_Engineering=0;
if RtOP___Program___Project_Managem=. then RtOP___Program___Project_Managem=0;
if RtOP___Security=. then RtOP___Security=0;
if RtOP___Software_Services=. then RtOP___Software_Services=0;
if RtOP___Storage_Backup=. then RtOP___Storage_Backup=0;
if RtOP___Utility_Services_Cloud=. then RtOP___Utility_Services_Cloud=0;
if RtOP___WorkPlace_Services=. then RtOP___WorkPlace_Services=0;
if IM___Critical_CaseCount=. then IM___Critical_CaseCount=0;
if IM___High_CaseCount=. then IM___High_CaseCount=0;
if IM___Others_CaseCount=. then IM___Others_CaseCount=0;
if IM___Critical_TTRMetNotCaseCount=. then IM___Critical_TTRMetNotCaseCount=0;
if IM___High_TTRMetNotCaseCount=. then IM___High_TTRMetNotCaseCount=0;
if IM___Others_TTRMetNotCaseCount=. then IM___Others_TTRMetNotCaseCount=0;
if IM___APPS=. then IM___APPS=0;
if IM___Cross_Functional___Multiple=. then IM___Cross_Functional___Multiple=0;
if IM___EAO=. then IM___EAO=0;
if IM___ECSO=. then IM___ECSO=0;
if IM___ESM=. then IM___ESM=0;
if IM___GSD=. then IM___GSD=0;
if IM___HPIT=. then IM___HPIT=0;
if IM___ITO_OTHER=. then IM___ITO_OTHER=0;
if IM___Mainframe=. then IM___Mainframe=0;
if IM___Networking_Services=. then IM___Networking_Services=0;
if IM___Security_Services=. then IM___Security_Services=0;
if IM___Software_Services=. then IM___Software_Services=0;
if IM___Storage__Backup=. then IM___Storage__Backup=0;
if IM___UNIX_LINUX=. then IM___UNIX_LINUX=0;
if IM___Wintel=. then IM___Wintel=0;
if IM___WorkPlace_Services=. then IM___WorkPlace_Services=0;
if ABS_TOTAL_COUNT=. then ABS_TOTAL_COUNT=0;
if ABS_UTILIZATION_RATE=. then ABS_UTILIZATION_RATE=0;
if ABS_SINGLEACCOUNT_UTILIZATION_RA=. then ABS_SINGLEACCOUNT_UTILIZATION_RA=0;
if ABS_HIGH_COST_COUNTRY_COUNT=. then ABS_HIGH_COST_COUNTRY_COUNT=0;
if ABS_HIGH_COST_COUNTRY_UTILIZATIO=. then ABS_HIGH_COST_COUNTRY_UTILIZATIO=0;
if ABS_LOW_COST_COUNTRY_COUNT=. then ABS_LOW_COST_COUNTRY_COUNT=0;
if ABS_LOW_COST_COUNTRY_UTILIZATION=. then ABS_LOW_COST_COUNTRY_UTILIZATION=0;
if ABS_GLOBAL_CENTER_COUNT=. then ABS_GLOBAL_CENTER_COUNT=0;
if ABS_GLOBAL_CENTER_UTILIZATION=. then ABS_GLOBAL_CENTER_UTILIZATION=0;
if ABS_LOCAL_DELIVERY_COUNT=. then ABS_LOCAL_DELIVERY_COUNT=0;
if ABS_LOCAL_DELIVERY_UTILIZATION=. then ABS_LOCAL_DELIVERY_UTILIZATION=0;
if ABS_REGIONAL_CENTER_COUNT=. then ABS_REGIONAL_CENTER_COUNT=0;
if ABS_REGIONAL_CENTER_UTILIZATION=. then ABS_REGIONAL_CENTER_UTILIZATION=0;
if ABS_VOLUMETRIC_SERVICE_COUNT=. then ABS_VOLUMETRIC_SERVICE_COUNT=0;
if ABS_VOLUMETRIC_SERVICE_UTILIZATI=. then ABS_VOLUMETRIC_SERVICE_UTILIZATI=0;
if ABS_Internal_COUNT=. then ABS_Internal_COUNT=0;
if ABS_INTERNAL_UTILIZATION=. then ABS_INTERNAL_UTILIZATION=0;
if ABS_VOLUMETRIC_DEMAND_COUNT=. then ABS_VOLUMETRIC_DEMAND_COUNT=0;
if ABS_VOLUMETRIC_DEMAND_UTILIZATIO=. then ABS_VOLUMETRIC_DEMAND_UTILIZATIO=0;
if ABS_ACCOUNT_FUNDED_FTE_COUNT=. then ABS_ACCOUNT_FUNDED_FTE_COUNT=0;
if ABS_ACCOUNT_FUNDED_FTE_UTILIZATI=. then ABS_ACCOUNT_FUNDED_FTE_UTILIZATI=0;
if ABS_Overhead_COUNT=. then ABS_Overhead_COUNT=0;
if ABS_Overhead_UTILIZATION=. then ABS_Overhead_UTILIZATION=0;

run;

*/
