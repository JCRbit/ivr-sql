CREATE OR REPLACE TABLE keepcoding.ivr_summary 
AS
  WITH calls_documents 
       -- Get unique document type and document identification by call
    AS (
          SELECT calls_ivr_id, 
                 document_type, 
                 document_identification
            FROM keepcoding.ivr_detail
           WHERE document_type <> "NULL" OR document_identification <> "NULL"
        GROUP BY calls_ivr_id, 
                 document_type, 
                 document_identification
         QUALIFY ROW_NUMBER() OVER(PARTITION BY CAST(calls_ivr_id AS STRING) 
                                       ORDER BY NULLIF(document_type, "NULL") ASC NULLS LAST) = 1
    )

  SELECT ivr_detail.calls_ivr_id AS ivr_id,
         ivr_detail.calls_phone_number AS phone_number,
         ivr_detail.calls_ivr_result AS ivr_result,
         CASE WHEN STARTS_WITH(ivr_detail.calls_vdn_label, "ATC") THEN "FRONT"
              WHEN STARTS_WITH(ivr_detail.calls_vdn_label, "TECH") THEN "TECH"
              WHEN ivr_detail.calls_vdn_label = "ABSORPTION" THEN "ABSORPTION"  
              ELSE "RESTO"
         END AS vdn_aggregation,
         ivr_detail.calls_start_date AS start_date,
         ivr_detail.calls_end_date AS end_date,
         ivr_detail.calls_total_duration AS total_duration,
         ivr_detail.calls_customer_segment AS customer_segment,
         ivr_detail.calls_ivr_language AS ivr_language,
         ivr_detail.calls_steps_module AS steps_module,
         ivr_detail.calls_module_aggregation AS module_aggregation,
         IFNULL(calls_documents.document_type, "NULL") AS document_type,
         IFNULL(calls_documents.document_identification, "NULL") AS document_identification,
         IFNULL(MIN(NULLIF(ivr_detail.customer_phone, "NULL")), "NULL") AS customer_phone,
         IFNULL(MIN(NULLIF(ivr_detail.billing_account_id, "NULL")), "NULL") AS billing_account_id,
        
         -- Flag if call goes through "AVERIA_MASIVA" module
         IF(COUNTIF(ivr_detail.module_name = "AVERIA_MASIVA") > 0, 1, 0) AS masiva_lg,
        
         -- Flag if client has been identified by phone
         COUNTIF(ivr_detail.step_name = "CUSTOMERINFOBYPHONE.TX" 
                 AND ivr_detail.step_description_error = "NULL") AS info_by_phone_lg,
        
         -- Flag if client has been identified by identity document
         COUNTIF(ivr_detail.step_name = "CUSTOMERINFOBYDNI.TX" 
                 AND ivr_detail.step_description_error = "NULL") AS info_by_dni_lg,
        
         -- Flag if the previous call was made within the previous 24 hours
         IF(TIMESTAMP_DIFF(ivr_detail.calls_start_date, 
                           LAG(ivr_detail.calls_start_date)
                              OVER (PARTITION BY ivr_detail.calls_phone_number 
                                        ORDER BY ivr_detail.calls_start_date ASC), 
                           SECOND) <= 86400, 1, 0) AS repeated_phone_24H,
        
         -- Flag if the following call was made within the next 24 hours
         IF(TIMESTAMP_DIFF(LEAD(ivr_detail.calls_start_date)
                              OVER (PARTITION BY ivr_detail.calls_phone_number 
                                        ORDER BY ivr_detail.calls_start_date ASC),
                           ivr_detail.calls_start_date, 
                           SECOND) <= 86400, 1, 0) AS cause_recall_phone_24H
    FROM keepcoding.ivr_detail
    LEFT 
    JOIN calls_documents
      ON ivr_detail.calls_ivr_id = calls_documents.calls_ivr_id
GROUP BY ivr_id,
         phone_number,
         ivr_result,
         vdn_aggregation,
         start_date,
         end_date,
         total_duration,
         customer_segment,
         ivr_language,
         steps_module,
         module_aggregation,
         document_type,
         document_identification

