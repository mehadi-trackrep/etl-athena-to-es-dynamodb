from datetime import datetime

def get_utc_time():
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

def get_athena_source_query() -> str:

    query = f"""
with raw_data as (
    SELECT
      -- Identifiers
      Orgnr AS orgno,
      
      -- Vehicle Status and Type
      Fordonsstatus AS vehicle_status,
      Fordonstyp AS vehicle_type,
      Marke AS brand,
      Fordonsar AS vehicle_year,
      Modellar AS model_year,
      
      -- Key Dates
      Senast_agarbyte AS last_ownership_change,
      Forregistrerad AS pre_registered_date,
      Forst_i_trafik AS first_in_traffic_date,
      Forst_pa_svenska_vagar AS first_on_roads_date,
      Nasta_besiktning_senast AS next_inspection_due_date,
      
      -- Boolean Flags
      CASE WHEN lower(Registrerad_for_yrkestrafik) IN ('ja', 'true', '1') THEN true ELSE false END AS registered_for_commercial_traffic,
      CASE WHEN lower(Importerad) IN ('ja', 'true', '1') THEN true ELSE false END AS imported,
      CASE WHEN lower(Leasing) IN ('ja', 'true', '1') THEN true ELSE false END AS leasing,
      CASE WHEN lower(Kopt_pa_kredit) IN ('ja', 'true', '1') THEN true ELSE false END AS purchased_on_credit,
      
      -- Odometer
      Matarstallning AS odometer_reading,
      Matarstallning_enhet AS odometer_unit,
      
      -- Fuel Types
      Drivmedel_1 AS fuel_type_1,
      Drivmedel_2 AS fuel_type_2,
      Drivmedel_3 AS fuel_type_3
    
    FROM
      "AwsDataCatalog"."vehicle_data"."vehicle_data"
    
    WHERE
      cc = 'se' AND d = '2025-08-27'
)


select 
    orgno
    , ARRAY_AGG(
        cast (
            CAST(
              ROW(
                cast(orgno as bigint),
				vehicle_status,
				vehicle_type,
				brand,
				try_cast(vehicle_year as bigint),
				try_cast(model_year as bigint),
				last_ownership_change,
				try_cast(pre_registered_date as date),
				try_cast(first_in_traffic_date as date),
				try_cast(first_on_roads_date as date),
				try_cast(next_inspection_due_date as date),
				registered_for_commercial_traffic,
				try_cast(imported as boolean),
				try_cast(leasing as boolean),
				try_cast(purchased_on_credit as boolean),
				try_cast(odometer_reading as bigint),
				odometer_unit,
				fuel_type_1,
				fuel_type_2,
				fuel_type_3
			) 
              AS 
              ROW(
                orgno bigint,
				vehicle_status varchar,
				vehicle_type varchar,
				brand varchar,
				vehicle_year bigint,
				model_year bigint,
				last_ownership_change varchar,
				pre_registered_date date,
				first_in_traffic_date date,
				first_on_roads_date date,
				next_inspection_due_date date,
				registered_for_commercial_traffic varchar,
				imported boolean,
				leasing boolean,
				purchased_on_credit boolean,
				odometer_reading bigint,
				odometer_unit varchar,
				fuel_type_1 varchar,
				fuel_type_2 varchar,
				fuel_type_3 varchar

              )
            )
            as json
        )
      ) as child_data
from raw_data
group by orgno
    """
    
    return query