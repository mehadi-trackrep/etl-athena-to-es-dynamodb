
def get_athena_source_query() -> str:

    query = f"""
with 

se_ie_companies_dont_have_crm_or_lists as (
SELECT orgno,
    company_name,
    count(*) over(partition by company_name) as total_count
FROM "prod_proff_data"."es_required_company_field_dump_json"
where d='2025-08-24'
    and country_code='SE'
    and strpos(organisation_group_labels, 'OTINDIVIDUALENTREPRENEURSHIP') > 0
    and (company_lists is null or trim(company_lists) = '')
    and (company_in_crm is null or trim(company_in_crm) = '')
    -- and (marked_as_client_by_account is null or trim(marked_as_client_by_account) = '')
),

se_ie_companies_dont_have_crm_or_lists_v2 as (
SELECT orgno,
    company_name,
    count(*) over(partition by company_name) as total_count
FROM "prod_proff_data"."es_required_company_field_dump_json"
where d='2025-08-24'
    and country_code='SE'
    and strpos(organisation_group_labels, 'OTINDIVIDUALENTREPRENEURSHIP') > 0
    and (company_lists is null or trim(company_lists) = '')
    and (company_in_crm is null or trim(company_in_crm) = '')
    and (marked_as_client_by_account is null or trim(marked_as_client_by_account) = '')
)

, new_se_ie_companies as (
select organisationnumber as orgno
    , sorganisationnumber
from "prod_proff_data"."proff_raw_data_iceberg"
where country_code='se'
    and companytype in ('Enskild näringsidkare')
    and try_cast(sorganisationnumber as bigint) is not null
    and organisationnumber = try_cast(sorganisationnumber as bigint)
)

, we_have_run as (
select a.orgno
    , a.company_name
    , a.total_count
from se_ie_companies_dont_have_crm_or_lists a
    left join new_se_ie_companies b on (
        a.orgno = b.orgno
    )
where total_count = 2 and b.orgno is null
)

, we_should_run as (
select a.orgno
    , a.company_name
    , a.total_count
from se_ie_companies_dont_have_crm_or_lists_v2 a
    left join new_se_ie_companies b on (
        a.orgno = b.orgno
    )
where total_count = 2 and b.orgno is null
)

select a.orgno
, 'ACTIVE' as status
from we_have_run a 
left join we_should_run b on (a.orgno=b.orgno)
where b.orgno is null
    """
    
    return query