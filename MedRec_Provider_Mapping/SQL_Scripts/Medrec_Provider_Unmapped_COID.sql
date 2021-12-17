select distinct
	t.data_server_code as Data_Server_code
	,prov.prv_nm as Provider_Name
	,t.trn_prv_cd as Provider_Code
	,efac.fac_grp as Fac_Grp
	,t.trn_fac_cd as Fac_Cd
	,efac.fac_nm as Fac_Nm
	,co.Coid
	,co.DeptCode
from edwps_staging.mr_trn_file_hist  t  
join edwps_staging.mr_enc_file_hist e
	on t.trn_enc_num = e.enc_num
	and t.data_server_code = e.data_server_code
left join edwps_staging.mr_fac_file_hist efac 
	on  t.trn_fac_cd = efac.fac_cd 
	and t. data_server_code = efac.data_server_code
left join edwps_staging.mr_prv_file_hist prov  
	on  t.trn_prv_cd = prov.prv_cd 
	and t. data_server_code = prov.data_server_code
left join edwps_staging.mr_ent_coid_mgt co
	on  co.data_server_code =t.data_server_code
	and co.provider_id_conv= t.trn_prv_cd   
	and co.location_id_conv=efac.fac_grp    
left join edwps_base_views.lu_date lu
	on t.trn_post_dt = lu.date_id
where		
	t.trn_amt not = 0
	and co.coid is null		
	and e.enc_pat_acct not = 0
	and lu.month_id >= extract(year from add_months(current_timestamp(0), -6)) * 100  + extract(month from add_months(current_timestamp(0), -6))
	and prov.prv_nm not= 'unassigned doctor'	
order by 1,2,3,4,5