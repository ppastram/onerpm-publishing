import functions_framework
from google.cloud import bigquery
from google.cloud import storage

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def main(cloud_event):
    data = cloud_event.data
    print(cloud_event)
    if "finalized" in cloud_event["type"] :

        event_id = cloud_event["id"]
        event_type = cloud_event["type"]

        bucket = data["bucket"]
        name = data["name"]
        metageneration = data["metageneration"]
        timeCreated = data["timeCreated"]
        updated = data["updated"]

        client = bigquery.Client()
        if "top_residual_artists_metadata" in name:

            #Set table_id to the ID of the table to create.
            table_id = f"unclaimedroyalties.yt_dsr.yt_unclaimed_artists_metadata"


            job_config = bigquery.LoadJobConfig(
                autodetect=True, #Automatic schema
                field_delimiter="\t", # Use \t if your separator is tab in your TSV file
                #skip_leading_rows=0, #Skip the header values(but keep it for the column naming)
                # The source format defaults to CSV, so the line below is optional.
                source_format=bigquery.SourceFormat.CSV,
                write_disposition = 'WRITE_APPEND',
                max_bad_records = 0,
                allow_quoted_newlines=True
            )
            job_config.quote_character = ""
            uri = "gs://{}/{}".format(bucket, name)

            print(f"Loading {uri}")

            load_job = client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )  # Make an API request.

            load_job.result()  # Waits for the job to complete.
            print("Residual metadata uploaded successfully")
            return("Residual metadata uploaded successfully")

        name_dict = {
            'ads': 'ads',
            'hardware': 'hardware',
            'subscriptionpremium': 'subs_premium',
            'subscriptionmusic': 'subs_music'
        }
        abbreviated_name = ""

        for file_name in name_dict.keys():
            if file_name in name.lower():
                abbreviated_name = name_dict[file_name]
                break
        

        print(f"Event ID: {event_id}")
        print(f"Event type: {event_type}")
        print(f"Bucket: {bucket}")
        print(f"File: {name}")
        print(f"Metageneration: {metageneration}")
        print(f"Created: {timeCreated}")
        print(f"Updated: {updated}")
        # Construct a BigQuery client object.
        client = bigquery.Client()
        file_date = name.split("/")[1]
        year = file_date.split("_")[0]
        quarter = file_date.split("_")[1]

        
        #Set table_id to the ID of the table to create.
        table_id = f"unclaimedroyalties.yt_dsr.{abbreviated_name}"


        job_config = bigquery.LoadJobConfig(
            autodetect=True, #Automatic schema
            field_delimiter="\t", # Use \t if your separator is tab in your TSV file
            #skip_leading_rows=0, #Skip the header values(but keep it for the column naming)
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
            write_disposition = 'WRITE_TRUNCATE',
            max_bad_records = 1000,
            allow_quoted_newlines=True
        )
        job_config.quote_character = ""
        uri = "gs://{}/{}".format(bucket, name)

        print(f"Loading {uri}")

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        
        dataset_ref = client.dataset("yt_dsr", project="unclaimedroyalties")
        table_ref = dataset_ref.table(f"{abbreviated_name}")
        table = client.get_table(table_ref)
        string_field_25_exists = any(field.name == 'string_field_25' for field in table.schema)

        if string_field_25_exists:
            query = f"""
            
            -- Create table for SY05.03 records
            CREATE TABLE unclaimedroyalties.yt_dsr.temp_SY03 AS
            SELECT
                string_field_1 AS SummaryRecordId,
                string_field_2 AS DistributionChannel,
                string_field_3 AS DistributionChannelDPID,
                string_field_4 AS CommercialModel,
                string_field_5 AS UseType,
                string_field_6 AS Territory,
                string_field_7 AS ServiceDescription,
                string_field_8 AS RightsController,
                string_field_9 AS RightsControllerPartyId,
                string_field_10 AS RightsType,
                CAST(string_field_11 AS INT64) AS TotalUsages,
                CAST(string_field_12 AS FLOAT64) AS AllocatedUsages,
                CAST(string_field_13 AS FLOAT64) AS MusicUsagePercentage,
                CAST(string_field_14 AS FLOAT64) AS AllocatedNetRevenue,
                CAST(string_field_15 AS FLOAT64) AS AllocatedRevenue,
                CAST(string_field_16 AS FLOAT64) AS RightsControllerMarketShare,
                string_field_17 AS CurrencyOfReporting,
                string_field_18 AS CurrencyOfTransaction,
                CAST(string_field_19 AS FLOAT64) AS ExchangeRate,
                string_field_20 AS SubscriberType,
                string_field_21 AS SubPeriodStartDate,
                string_field_22 AS SubPeriodEndDate,
                string_field_23 AS ContentCategory,
                CAST(string_field_24 AS FLOAT64) AS RightsTypePercentage,
                string_field_25 AS ParentSummaryRecordId,
                '{file_date}' AS Quarter
            FROM
                unclaimedroyalties.yt_dsr.{abbreviated_name}
            WHERE
                string_field_0 = 'SY05.03';
            
            CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_SY03 AS
            SELECT * 
            FROM unclaimedroyalties.yt_dsr.temp_SY03
            WHERE FALSE;

            INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_SY03
            SELECT * FROM unclaimedroyalties.yt_dsr.temp_SY03;

            CREATE OR REPLACE TABLE unclaimedroyalties.yt_dsr.{abbreviated_name}_SY03 as (
            SELECT DISTINCT *
            from unclaimedroyalties.yt_dsr.{abbreviated_name}_SY03);

            DROP TABLE unclaimedroyalties.yt_dsr.temp_SY03;
            """           
        else:
            query = f"""
            -- Create table for SY02.03 records
            CREATE TABLE unclaimedroyalties.yt_dsr.temp_SY03 AS
            (SELECT
                string_field_1 AS SummaryRecordId,
                string_field_2 AS DistributionChannel,
                string_field_3 AS DistributionChannelDPID,
                string_field_4 AS CommercialModel,
                string_field_5 AS UseType,
                string_field_6 AS Territory,
                string_field_7 AS ServiceDescription,
                CAST(string_field_8 AS INT64) AS TotalUsages,
                CAST(string_field_9 AS INT64) AS Users,
                string_field_10 AS CurrencyOfReporting,
                CAST(string_field_11 AS FLOAT64) AS NetRevenue,
                string_field_12 AS RightsController,
                string_field_13 AS RightsControllerPartyId,
                CAST(string_field_14 AS FLOAT64) AS AllocatedUsages,
                CAST(string_field_15 AS FLOAT64) AS AllocatedRevenue,
                CAST(string_field_16 AS FLOAT64) AS AllocatedNetRevenue,
                string_field_17 AS RightsType,
                string_field_18 AS ContentCategory,
                string_field_19 AS CurrencyOfTransaction,
                CAST(string_field_20 AS FLOAT64) AS ExchangeRate,
                CAST(string_field_21 AS FLOAT64) AS RightsTypePercentage,
                '{file_date}' AS Quarter
            FROM
                unclaimedroyalties.yt_dsr.{abbreviated_name}
            WHERE
                string_field_0 = 'SY02.03'
            );

            CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_SY03 AS
            SELECT * 
            FROM unclaimedroyalties.yt_dsr.temp_SY03
            WHERE FALSE;

            INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_SY03
            SELECT * FROM unclaimedroyalties.yt_dsr.temp_SY03;

            CREATE OR REPLACE TABLE unclaimedroyalties.yt_dsr.{abbreviated_name}_SY03 as (
            SELECT DISTINCT *
            from unclaimedroyalties.yt_dsr.{abbreviated_name}_SY03);

            DROP TABLE unclaimedroyalties.yt_dsr.temp_SY03;
            """

        print("Loading SY table")
        query_job = client.query(query)
        query_job.result()

        query = f""" 
        -- Create table for HEAD records
        CREATE TABLE unclaimedroyalties.yt_dsr.temp_HEAD AS
        SELECT
            string_field_1 AS MessageVersion,
            string_field_2 AS Profile,
            string_field_3 AS ProfileVersion,
            string_field_4 AS MessageId,
            string_field_5 AS MessageCreatedDateTime,
            CAST(string_field_6 AS INT64) AS FileNumber,
            CAST(string_field_7 AS INT64) AS NumberOfFiles,
            CAST(string_field_8 AS DATE) AS UsageStartDate,
            CAST(string_field_9 AS DATE) AS UsageEndDate,
            string_field_10 AS SenderPartyId,
            string_field_11 AS SenderName,
            string_field_12 AS ServiceDescription,
            string_field_13 AS RecipientPartyId,
            string_field_14 AS RecipientName,
            string_field_15 AS RepresentedRepertoire,
            '{file_date}' AS Quarter
        FROM
            unclaimedroyalties.yt_dsr.{abbreviated_name}
        WHERE
            string_field_0 = 'HEAD';
        
        CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_HEAD AS
        SELECT * 
        FROM unclaimedroyalties.yt_dsr.temp_HEAD
        WHERE FALSE;

        INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_HEAD
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_HEAD;

        DROP TABLE unclaimedroyalties.yt_dsr.temp_HEAD;


        -- Create table for AS01.02 records
        CREATE TABLE unclaimedroyalties.yt_dsr.temp_AS0102 AS
        SELECT
            string_field_1 AS BlockId,
            string_field_2 AS ResourceReference,
            string_field_3 AS DspResourceId,
            string_field_4 AS ResourceISRC,
            string_field_5 AS ResourceTitle,
            string_field_6 AS ResourceSubTitle,
            string_field_7 AS ResourceDisplayArtistName,
            string_field_8 AS ResourceDisplayArtistPartyId,
            string_field_9 AS ResourceDuration,
            string_field_10 AS ResourceType,
            string_field_11 AS IsMasterRecording,
            string_field_12 AS IsSubjectToOwnershipConflict,
            string_field_13 AS LastConflictCheck,
            '{file_date}' AS Quarter
        FROM
            unclaimedroyalties.yt_dsr.{abbreviated_name}
        WHERE
            string_field_0 = 'AS01.02';
        
        CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_AS0102 AS
        SELECT * 
        FROM unclaimedroyalties.yt_dsr.temp_AS0102
        WHERE FALSE;

        INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_AS0102
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_AS0102;

        DROP TABLE unclaimedroyalties.yt_dsr.temp_AS0102;


        -- Create table for RU01.02 records
        CREATE TABLE unclaimedroyalties.yt_dsr.temp_RU0102 AS
        SELECT
            string_field_1 AS BlockId,
            string_field_2 AS SummaryRecordId,
            string_field_3 AS DspReleaseId,
            string_field_4 ASUsages,
            string_field_5 AS ContentCategory,
            string_field_6 AS ParentResourceReference,
            '{file_date}' AS Quarter
        FROM
            unclaimedroyalties.yt_dsr.{abbreviated_name}
        WHERE
            string_field_0 = 'RU01.02';
        
        CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_RU0102 AS
        SELECT * 
        FROM unclaimedroyalties.yt_dsr.temp_RU0102
        WHERE FALSE;

        INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_RU0102
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_RU0102;

        DROP TABLE unclaimedroyalties.yt_dsr.temp_RU0102;


        -- Create table for SU03.02 records
        CREATE TABLE unclaimedroyalties.yt_dsr.temp_SU0302 AS
        SELECT
            string_field_1 AS BlockId,
            string_field_2 AS SalesTransactionId,
            string_field_3 AS SummaryRecordId,
            string_field_4 AS DspResourceId,
            CAST(string_field_5 AS FLOAT64) AS Usages,
            CAST(string_field_6 AS FLOAT64) AS NetRevenue,
            string_field_7 AS ValidityPeriodStart,
            string_field_8 AS ValidityPeriodEnd,
            string_field_9 AS ContentCategory,
            string_field_10 AS IsRoyaltyBearing,
            '{file_date}' AS Quarter
        FROM
            unclaimedroyalties.yt_dsr.{abbreviated_name}
        WHERE
            string_field_0 = 'SU03.02';

        CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_SU0302 AS
        SELECT * 
        FROM unclaimedroyalties.yt_dsr.temp_SU0302
        WHERE FALSE;

        INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_SU0302
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_SU0302;

        DROP TABLE unclaimedroyalties.yt_dsr.temp_SU0302;


        -- Create table for LI01.03 records
        CREATE TABLE unclaimedroyalties.yt_dsr.temp_LI0103 AS
        SELECT
            string_field_1 AS BlockId,
            string_field_2 AS SummaryRecordId,
            string_field_3 AS RightsController,
            string_field_4 AS RightsControllerPartyId,
            string_field_5 AS RightsControllerWorkId,
            CAST(string_field_6 AS FLOAT64) AS RightSharePercent,
            string_field_7 AS RightsType,
            CAST(string_field_8 AS FLOAT64) AS AllocatedNetRevenue,
            CAST(string_field_9 AS FLOAT64) AS AllocatedAmount,
            CAST(string_field_10 AS FLOAT64) AS AllocatedUsages,
            string_field_11 AS ParentSalesTransactionId,
            string_field_12 AS LicensorDataRecordId,
            '{file_date}' AS Quarter
        FROM
            unclaimedroyalties.yt_dsr.{abbreviated_name}
        WHERE
            string_field_0 = 'LI01.03';

        CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_LI0103 AS
        SELECT * 
        FROM unclaimedroyalties.yt_dsr.temp_LI0103
        WHERE FALSE;

        INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_LI0103
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_LI0103;

        DROP TABLE unclaimedroyalties.yt_dsr.temp_LI0103;


        -- Create table for MW01.02 records
        CREATE TABLE unclaimedroyalties.yt_dsr.temp_MW0102 AS
        SELECT
            string_field_1 AS BlockId,
            string_field_2 AS DspMusicalWorkId,
            string_field_3 AS MusicalWorkISWC,
            string_field_4 AS MusicalWorkTitle,
            string_field_5 AS MusicalWorkSubTitle,
            string_field_6 AS MusicalWorkComposerAuthorName,
            string_field_7 AS MusicalWorkComposerAuthorPartyId,
            string_field_8 AS MusicalWorkArrangerName,
            string_field_9 AS MusicalWorkArrangerPartyId,
            string_field_10 AS MusicPublisherName,
            string_field_11 AS MusicPublisherPartyId,
            string_field_12 AS MusicalWorkContributorName,
            string_field_13 AS MusicalWorkContributorPartyId,
            string_field_14 AS DataProviderName,
            string_field_15 AS ProprietaryMusicalWorkId,
            string_field_16 AS ResourceReference,
            string_field_17 AS ParentLicensorDataRecordId,
            string_field_18 AS ParentMasterlistId,
            '{file_date}' AS Quarter
        FROM
            unclaimedroyalties.yt_dsr.{abbreviated_name}
        WHERE
            string_field_0 = 'MW01.02';

        
        CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_MW0102 AS
        SELECT * 
        FROM unclaimedroyalties.yt_dsr.temp_MW0102
        WHERE FALSE;

        INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_MW0102
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_MW0102;

        DROP TABLE unclaimedroyalties.yt_dsr.temp_MW0102;

        
            -- Create table for LC01 records
        CREATE TABLE unclaimedroyalties.yt_dsr.temp_LC01 AS
        SELECT
            string_field_1 AS BlockId,
            string_field_2 AS ResourceReference,
            string_field_3 AS DspResourceId,
            string_field_4 AS ISRC,
            string_field_5 AS ResourceTitle,
            string_field_6 AS ResourceSubTitle,
            string_field_7 AS ResourceDisplayArtistName,
            string_field_8 AS ResourceDisplayArtistPartyId,
            string_field_9 AS ResourceDuration,
            string_field_10 AS ResourceType,
            string_field_11 AS MusicalWorkISWC,
            string_field_12 AS MusicalWorkComposerAuthorName,
            string_field_13 AS MusicalWorkComposerAuthorPartyId,
            string_field_14 AS MusicalWorkArrangerName,
            string_field_15 AS MusicalWorkArrangerPartyId,
            string_field_16 AS MusicPublisherName,
            string_field_17 AS MusicPublisherPartyId,
            string_field_18 AS MusicalWorkContributorName,
            string_field_19 AS MusicalWorkContributorPartyId,
            string_field_20 AS ProprietaryMusicalWorkId,
            string_field_21 AS DataProviderName,
            string_field_22 AS IsSubjectToOwnershipConflict,
            '{file_date}' AS Quarter
        FROM
            unclaimedroyalties.yt_dsr.{abbreviated_name}
        WHERE
            string_field_0 = 'LC01';
        
        CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_LC01 AS
        SELECT * 
        FROM unclaimedroyalties.yt_dsr.temp_LC01
        WHERE FALSE;

        INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_LC01
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_LC01;

        DROP TABLE unclaimedroyalties.yt_dsr.temp_LC01;
        
        
        -- Create table for SY04.02 records -- this one applies for Subscription Only
        CREATE TABLE unclaimedroyalties.yt_dsr.temp_SY0402 AS
        SELECT
            string_field_1 AS SummaryRecordId,
            string_field_2 AS DistributionChannel,
            string_field_3 AS DistributionChannelDPID,
            string_field_4 AS CommercialModel,
            string_field_5 AS UseType,
            string_field_6 AS Territory,
            string_field_7 AS ServiceDescription,
            string_field_8 AS SubscriberType,
            CAST(string_field_9 AS INT64) AS Subscribers,
            string_field_10 AS SubPeriodStartDate,
            string_field_11 AS SubPeriodEndDate,
            CAST(string_field_12 AS INT64) AS TotalUsagesInSubPeriod,
            CAST(string_field_13 AS INT64) AS TotalUsagesInReportingPeriod,
            string_field_14 AS CurrencyOfReporting,
            string_field_15 AS CurrencyOfTransaction,
            CAST(string_field_16 AS FLOAT64) AS ExchangeRate,
            CAST(string_field_17 AS FLOAT64) AS ConsumerPaidUnitPrice,
            CAST(string_field_18 AS FLOAT64) AS NetRevenue,
            CAST(string_field_19 AS FLOAT64) AS MusicUsagePercentage,
            '{file_date}' AS Quarter
        FROM
            unclaimedroyalties.yt_dsr.{abbreviated_name}
        WHERE
            string_field_0 = 'SY04.02';
        
        CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_SY0402 AS
        SELECT * 
        FROM unclaimedroyalties.yt_dsr.temp_SY0402
        WHERE FALSE;

        INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_SY0402
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_SY0402;

        DROP TABLE unclaimedroyalties.yt_dsr.temp_SY0402;

        
        -- Create table for SY09.01 records
        CREATE TABLE unclaimedroyalties.yt_dsr.temp_SY0901 AS
        SELECT
            string_field_1 AS SummaryRecordId,
            string_field_2 AS CommercialModel,
            string_field_3 AS UseType,
            string_field_4 AS Territory,
            string_field_5 AS ServiceDescription,
            string_field_6 AS SubscriberType,
            string_field_7 AS RightsController,
            string_field_8 AS RightsControllerPartyId,
            string_field_9 AS RightsType,
            CAST(string_field_10 AS FLOAT64) AS TotalUsages,
            CAST(string_field_11 AS FLOAT64) AS AllocatedUsages,
            CAST(string_field_12 AS FLOAT64) AS NetRevenue,
            CAST(string_field_13 AS FLOAT64) AS IndirectValue,
            string_field_14 AS RightsControllerMarketShare,
            string_field_15 AS CurrencyOfReporting,
            string_field_16 AS CurrencyOfTransaction,
            string_field_17 AS ExchangeRate,
            string_field_18 AS RightsTypePercentage,
            string_field_19 AS SubPeriodStartDate,
            string_field_20 AS SubPeriodEndDate,
            string_field_21 AS ParentSummaryRecordId,
            '{file_date}' AS Quarter
        FROM
            unclaimedroyalties.yt_dsr.{abbreviated_name}
        WHERE
            string_field_0 = 'SY09.01';
        
        CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_SY0901 AS
        SELECT * 
        FROM unclaimedroyalties.yt_dsr.temp_SY0901
        WHERE FALSE;

        INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_SY0901
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_SY0901;

        DROP TABLE unclaimedroyalties.yt_dsr.temp_SY0901;
        

         -- Create table for ST01 records
        CREATE TABLE unclaimedroyalties.yt_dsr.temp_ST01 AS
        SELECT
            string_field_1 AS BlockId,
            string_field_2 AS ParentSalesTransactionId,
            string_field_3 AS SummaryRecordId,
            CAST(string_field_4 AS INT64) AS Usages,
            '{file_date}' AS Quarter
        FROM
            unclaimedroyalties.yt_dsr.{abbreviated_name}
        WHERE
            string_field_0 = 'ST01';
        
        CREATE TABLE IF NOT EXISTS unclaimedroyalties.yt_dsr.{abbreviated_name}_ST01 AS
        SELECT * 
        FROM unclaimedroyalties.yt_dsr.temp_ST01
        WHERE FALSE;

        INSERT INTO unclaimedroyalties.yt_dsr.{abbreviated_name}_ST01
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_ST01;

        DROP TABLE unclaimedroyalties.yt_dsr.temp_ST01;

        """
        print("Creating intermediate tables")
        query_job = client.query(query)
        query_job.result()


        # Initial temp_onerpm table creation
        query = f"""
        CREATE OR REPLACE TABLE unclaimedroyalties.yt_dsr.temp_onerpm AS
        SELECT
            a.BlockId,
            a.ResourceISRC,
            a.ResourceTitle,
            a.ResourceDisplayArtistName,
            a.Quarter,
            s.Usages,
            s.NetRevenue,
            s.ValidityPeriodStart,
            s.ValidityPeriodEnd,
            l.RightsController,
            l.RightsControllerWorkId,
            l.RightSharePercent,
            l.AllocatedNetRevenue,
            l.AllocatedAmount,
            l.AllocatedUsages,
            SAFE_DIVIDE(CAST(l.AllocatedUsages AS FLOAT64), 
                NULLIF((
                    SELECT SUM(AllocatedUsages) 
                    FROM `unclaimedroyalties.yt_dsr.{abbreviated_name}_LI0103` 
                    WHERE RightsController = 'ONErpm_BR_publishing'
                ), 0)
            ) AS UsagesShare,
            'YOUTUBE' AS PAYEE,
            CONCAT('YOUTUBE ', ' ', UPPER('{abbreviated_name}'), ' ', '{quarter}', ' ', '{year}') AS Distribution_Description,
            'youtube_comp' AS STORE_ID,
            '{name[-2:]}' AS COUNTRY_OF_SALE,
            'USD' AS CURRENCY,
            CAST(NULL AS FLOAT64) AS GEPaymentsShare,
            CAST(NULL AS FLOAT64) AS ROYALTIES_TO_BE_PAID,
            CAST(0 AS INT64) AS TOTAL_UNITS
        FROM `unclaimedroyalties.yt_dsr.{abbreviated_name}_AS0102` as a
        JOIN `unclaimedroyalties.yt_dsr.{abbreviated_name}_SU0302` as s 
            ON a.BlockId = s.BlockId AND a.Quarter = s.Quarter 
        JOIN `unclaimedroyalties.yt_dsr.{abbreviated_name}_LI0103` as l 
            ON a.BlockId = l.BlockId 
            AND a.Quarter = l.Quarter 
            AND l.ParentSalesTransactionId = s.SalesTransactionId
        WHERE l.RightsType = 'MechanicalRight'
            AND l.RightsController = 'ONErpm_BR_publishing';
        """
        print("Creating initial temp_onerpm table")
        query_job = client.query(query)
        query_job.result()

        # Update GEPaymentsShare and TOTAL_UNITS
        query = f"""
        UPDATE unclaimedroyalties.yt_dsr.temp_onerpm t
        SET 
            GEPaymentsShare = CAST((
                (
                    SELECT SUM(AllocatedRevenue) 
                    FROM unclaimedroyalties.yt_dsr.{abbreviated_name}_SY03 
                    WHERE RightsController = 'ONErpm_BR_publishing'
                ) - (
                    SELECT SUM(AllocatedAmount) 
                    FROM unclaimedroyalties.yt_dsr.{abbreviated_name}_LI0103 
                    WHERE RightsController = 'ONErpm_BR_publishing'
                )
            ) * t.UsagesShare AS FLOAT64),
            TOTAL_UNITS = CAST(CEIL(t.AllocatedUsages) AS INT64)

        WHERE TRUE;
        """
        print("Updating GEPaymentsShare and TOTAL_UNITS")
        query_job = client.query(query)
        query_job.result()

        # Update ROYALTIES_TO_BE_PAID
        query = f"""
        UPDATE unclaimedroyalties.yt_dsr.temp_onerpm t
        SET
            ROYALTIES_TO_BE_PAID = CAST((
                (
                    SELECT SUM(AllocatedRevenue)
                    FROM unclaimedroyalties.yt_dsr.{abbreviated_name}_SY03
                    WHERE RightsController = 'ONErpm_BR_publishing'
                ) - (
                    SELECT SUM(AllocatedAmount)
                    FROM unclaimedroyalties.yt_dsr.{abbreviated_name}_LI0103
                    WHERE RightsController = 'ONErpm_BR_publishing'
                )
            ) * t.UsagesShare AS FLOAT64) + AllocatedAmount)

        WHERE TRUE;
        """
        print("Updating ROYALTIES_TO_BE_PAID")
        query_job = client.query(query)
        query_job.result()

        # Rename columns and handle special cases
        query = f"""
        ALTER TABLE unclaimedroyalties.yt_dsr.temp_onerpm
        RENAME COLUMN ValidityPeriodEnd TO DATE_SALE;

        ALTER TABLE unclaimedroyalties.yt_dsr.temp_onerpm
        RENAME COLUMN RightsControllerWorkId TO PUBLISHERS_SONGCODE;

        UPDATE unclaimedroyalties.yt_dsr.temp_onerpm 
        SET PUBLISHERS_SONGCODE = REPLACE(PUBLISHERS_SONGCODE, 'C_', '')
        WHERE PUBLISHERS_SONGCODE LIKE 'C_%';

        ALTER TABLE unclaimedroyalties.yt_dsr.temp_onerpm
        RENAME COLUMN ResourceTitle TO SONG_TITLE;

        ALTER TABLE unclaimedroyalties.yt_dsr.temp_onerpm
        RENAME COLUMN ResourceISRC TO ISRC;
        """
        print("Renaming columns and updating PUBLISHERS_SONGCODE")
        query_job = client.query(query)
        query_job.result()

        # Get the year and quarter from the file_date
        year = file_date.split("_")[0]
        quarter = f"{file_date.split('_')[1]}"
        period = f"{year}{quarter}"


        # Create and populate the final report table
        query = f"""
        CREATE TABLE IF NOT EXISTS unclaimedroyalties.onerpm.yt_{abbreviated_name}_report (
            BlockId STRING,
            ISRC STRING,
            SONG_TITLE STRING,
            ResourceDisplayArtistName STRING,
            Quarter STRING,
            Usages FLOAT64,
            NetRevenue FLOAT64,
            ValidityPeriodStart STRING,
            DATE_SALE STRING,
            RightsController STRING,
            PUBLISHERS_SONGCODE STRING,
            RightSharePercent FLOAT64,
            AllocatedNetRevenue FLOAT64,
            AllocatedAmount FLOAT64,
            AllocatedUsages FLOAT64,
            UsagesShare FLOAT64,
            PAYEE STRING,
            Distribution_Description STRING,
            STORE_ID STRING,
            COUNTRY_OF_SALE STRING,
            CURRENCY STRING,
            GEPaymentsShare FLOAT64,
            ROYALTIES_TO_BE_PAID FLOAT64,
            TOTAL_UNITS INT64
        );

        INSERT INTO unclaimedroyalties.onerpm.yt_{abbreviated_name}_report
        SELECT * FROM unclaimedroyalties.yt_dsr.temp_onerpm;

        CREATE OR REPLACE TABLE `unclaimedroyalties.yt_dsr_tables.yt_ads_report_{period}` AS
        SELECT *
        FROM `unclaimedroyalties.onerpm.yt_{abbreviated_name}_report`;
        """
        print("Creating and populating final report table + historical table (inside yt_dsr_tables)")
        query_job = client.query(query)
        query_job.result()


        # Clean up
        query = "DROP TABLE unclaimedroyalties.yt_dsr.temp_onerpm;"
        print("Dropping temporary table")
        query_job = client.query(query)
        query_job.result()

                

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket)
        blobs = [x.name for x in bucket.list_blobs()]
        blobs = [blob for blob in blobs if file_date in blob]
        

        for file_name in name_dict.keys():        
            if file_name not in '\t'.join(blobs).lower():
                print(f"Not all files for date {file_date} are uploaded. At least one for {file_name} is missing")
                return f"Not all files for date {file_date} are uploaded. At least one for {file_name} is missing"
                break
        
        
        query = f""" 
        CREATE OR REPLACE TABLE `unclaimedroyalties.yt_dsr.merged` AS
        SELECT
            ResourceType,
            ResourceDisplayArtistName,
            BlockId,
            ResourceISRC,
            ResourceTitle,
            ResourceSubTitle,
            SalesTransactionId,
            RightsController,
            RightSharePercent,
            SUM(AllocatedUsages) as AUsages,
            SUM(AllocatedAmount) as AAmount,
            SUM(AllocatedNetRevenue) as ANetRevenue,
            SUM(Usages) as Usages,
            SUM(NetRevenue) as NetRevenue
        FROM (
                SELECT
                    ResourceType,
                    ResourceDisplayArtistName,
                    a.BlockId,
                    ResourceISRC,
                    ResourceTitle,
                    ResourceSubTitle,
                    SalesTransactionId,
                    RightsController,
                    RightSharePercent,
                    AllocatedUsages,
                    AllocatedAmount,
                    AllocatedNetRevenue,
                    Usages,
                    NetRevenue,
                    a.Quarter
                FROM `unclaimedroyalties.yt_dsr.ads_AS0102` as a
                JOIN `unclaimedroyalties.yt_dsr.ads_SU0302` as s on a.BlockId = s.BlockId AND a.Quarter = s.Quarter 
                JOIN `unclaimedroyalties.yt_dsr.ads_LI0103` as l on a.BlockId = l.BlockId AND a.Quarter = l.Quarter AND l.ParentSalesTransactionId = s.SalesTransactionId
                WHERE l.RightsType = 'MechanicalRight'
                
            UNION ALL
            
            SELECT
                ResourceType,
                ResourceDisplayArtistName,
                a.BlockId,
                ResourceISRC,
                ResourceTitle,
                ResourceSubTitle,
                SalesTransactionId,
                RightsController,
                RightSharePercent,
                AllocatedUsages,
                AllocatedAmount,
                AllocatedNetRevenue,
                Usages,
                NetRevenue,
                a.Quarter
            FROM `unclaimedroyalties.yt_dsr.subs_music_AS0102` as a
            JOIN `unclaimedroyalties.yt_dsr.subs_music_SU0302` as s on a.BlockId = s.BlockId AND a.Quarter = s.Quarter
            JOIN `unclaimedroyalties.yt_dsr.subs_music_LI0103` as l on a.BlockId = l.BlockId AND a.Quarter = l.Quarter AND l.ParentSalesTransactionId = s.SalesTransactionId
            WHERE l.RightsType = 'MechanicalRight'
            
            UNION ALL
            
            SELECT
                ResourceType,
                ResourceDisplayArtistName,
                a.BlockId,
                ResourceISRC,
                ResourceTitle,
                ResourceSubTitle,
                SalesTransactionId,
                RightsController,
                RightSharePercent,
                AllocatedUsages,
                AllocatedAmount,
                AllocatedNetRevenue,
                Usages,
                NetRevenue,
                a.Quarter
            FROM `unclaimedroyalties.yt_dsr.subs_premium_AS0102` as a
            JOIN `unclaimedroyalties.yt_dsr.subs_premium_SU0302` as s on a.BlockId = s.BlockId AND a.Quarter = s.Quarter
            JOIN `unclaimedroyalties.yt_dsr.subs_premium_LI0103` as l on a.BlockId = l.BlockId AND a.Quarter = l.Quarter AND l.ParentSalesTransactionId = s.SalesTransactionId
            WHERE l.RightsType = 'MechanicalRight'
            
            UNION ALL
            
            SELECT
                ResourceType,
                ResourceDisplayArtistName,
                a.BlockId,
                ResourceISRC,
                ResourceTitle,
                ResourceSubTitle,
                SalesTransactionId,
                RightsController,
                RightSharePercent,
                AllocatedUsages,
                AllocatedAmount,
                AllocatedNetRevenue,
                Usages,
                NetRevenue,
                a.Quarter
            FROM `unclaimedroyalties.yt_dsr.hardware_AS0102` as a
            JOIN `unclaimedroyalties.yt_dsr.hardware_SU0302` as s on a.BlockId = s.BlockId AND a.Quarter = s.Quarter
            JOIN `unclaimedroyalties.yt_dsr.hardware_LI0103` as l on a.BlockId = l.BlockId AND a.Quarter = l.Quarter AND l.ParentSalesTransactionId = s.SalesTransactionId
            WHERE l.RightsType = 'MechanicalRight'
        ) AS merged_data
        GROUP BY ResourceType, ResourceDisplayArtistName, BlockId, ResourceISRC, ResourceTitle, ResourceSubTitle, SalesTransactionId, RightsController, RightSharePercent;
        """
        print("Creating merged table")
        query_job = client.query(query)
        query_job.result()

        query = f""" 

        CREATE OR REPLACE TABLE unclaimedroyalties.yt_dsr.SplitArtists AS (
        SELECT
            TRIM(value) AS Artist,
            SUM(Usages) AS TotalUsages,
            SUM(AUsages)AS TotalAUsages,
            SUM(ANetRevenue) AS TotalANetRevenue,
            SUM(AAmount) AS TotalAAmount,
            SUM(NetRevenue) AS TotalNetRevenue
        FROM
            `unclaimedroyalties.yt_dsr.merged`,
        unnest(unclaimedroyalties.custom_functions.split_multi_delimiters(LOWER(ResourceDisplayArtistName))) as value
        WHERE
            ResourceDisplayArtistName IS NOT NULL
        AND
            value <> '-'
        GROUP BY
        Artist
        );

        CREATE OR REPLACE TABLE unclaimedroyalties.yt_dsr.SplitArtistsResidual AS (
        SELECT
            TRIM(value) AS Artist,
            AVG(RightSharePercent) AS AvgResidualRightsSharePctg,
            SUM(Usages) AS ResidualTotalUsages,
            SUM(AUsages)AS ResidualTotalAUsages,
            SUM(ANetRevenue) AS ResidualTotalANetRevenue,
            SUM(AAmount) AS ResidualTotalAAmount,
            SUM(NetRevenue) AS ResidualTotalNetRevenue,
            COUNT(Usages) AS ResidualObservations

        FROM
            `unclaimedroyalties.yt_dsr.merged`,
        unnest(unclaimedroyalties.custom_functions.split_multi_delimiters(LOWER(ResourceDisplayArtistName))) as value
        WHERE
            ResourceDisplayArtistName IS NOT NULL
        AND
            value <> '-'
        AND
            RightsController = 'Residual'
        GROUP BY Artist
        );

        CREATE OR REPLACE TABLE unclaimedroyalties.yt_dsr.max_values AS (
        SELECT
            MAX(ResidualObservations) AS MaxResidualObservations,
            MAX(ResidualTotalUsages) AS MaxResidualTotalUsages
        FROM
            unclaimedroyalties.yt_dsr.SplitArtistsResidual
        );


        CREATE OR REPLACE TABLE unclaimedroyalties.yt_dsr.top_residual_artists AS
        SELECT
        SplitArtists.Artist,
        AvgResidualRightsSharePctg,
        ResidualTotalUsages,
        ResidualTotalAUsages,
        ResidualTotalANetRevenue,
        ResidualTotalAAmount,
        ResidualTotalNetRevenue,
        ResidualObservations,
        (ResidualTotalUsages/NULLIF(TotalUsages, 0)) AS ResidualUsagesPctg,
        ((ResidualObservations /(MaxResidualObservations)) * 0.65) + ((ResidualTotalUsages / MaxResidualTotalUsages) * 0.35) AS ArtistIndex
        FROM
        unclaimedroyalties.yt_dsr.SplitArtists
        JOIN unclaimedroyalties.yt_dsr.SplitArtistsResidual ON SplitArtistsResidual.Artist = SplitArtists.Artist
        JOIN unclaimedroyalties.yt_dsr.max_values ON TRUE
        ORDER BY
        ArtistIndex DESC
        LIMIT 10000;


        CREATE OR REPLACE TABLE unclaimedroyalties.yt_dsr.split_artists AS (
            SELECT
                *,
                TRIM(value) AS Artist,
                FROM
                `unclaimedroyalties.yt_dsr.merged`,
            unnest(unclaimedroyalties.custom_functions.split_multi_delimiters(LOWER(ResourceDisplayArtistName))) as value
            WHERE
                ResourceDisplayArtistName IS NOT NULL
            AND
                value <> '-'
            );

        DROP TABLE IF EXISTS unclaimedroyalties.yt_dsr.top_residual_artists_songs;


        CREATE OR REPLACE TABLE unclaimedroyalties.yt_dsr.top_residual_artists_songs AS
          (SELECT
              a.*
          FROM
              (SELECT * FROM `unclaimedroyalties.yt_dsr.split_artists`) AS a
          JOIN
              `unclaimedroyalties.yt_dsr.top_residual_artists` AS t
          ON
              a.Artist = t.Artist);
            
        DROP TABLE unclaimedroyalties.yt_dsr.split_artists;
        DROP TABLE unclaimedroyalties.yt_dsr.SplitArtistsResidual;
        DROP TABLE unclaimedroyalties.yt_dsr.SplitArtists;
        DROP TABLE unclaimedroyalties.yt_dsr.max_values;
        DROP TABLE unclaimedroyalties.yt_dsr.merged;
        """
        print("Creating split tables")
        query_job = client.query(query)
        query_job.result()

        destination_uri = "gs://dsr_youtube/residual/top_residual_artists.csv" 
        dataset_ref = client.dataset("yt_dsr", project="unclaimedroyalties")
        table_ref = dataset_ref.table("top_residual_artists")

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            location='US') 
        extract_job.result() #Extracts results to the GCS

        print('Query results extracted to GCS: {}'.format(destination_uri))

        client.delete_table(table_ref) #Deletes table in BQ
