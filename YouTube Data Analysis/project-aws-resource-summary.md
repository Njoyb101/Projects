# Data Lake

## **RAW Layer :**

- **S3 Bucket Name :** `nj-yta-raw-apsouth1-dev`
- **S3 Bucket Data Types :** 
    - json data : `nj-yta-raw-apsouth1-dev/youtube/video_category_details_json/`
    - csv data (Partition by **Region**) : `nj-yta-raw-apsouth1-dev/youtube/video_details/` 

## **Athena Log Bucket :**   
- **S3 Bucket Name :** `nj-yta-raw-athena-logs`


## **Clean Layer for ETL :**   
- **S3 Bucket Name :** `nj-yta-clean-apsouth1-dev`
    - json : `s3://nj-yta-clean-apsouth1-dev/YT_video_category_details_json_normalized`
## **Analytics Layer :**   
- **S3 Bucket Name :** `nj-yta-analytics-apsouth1-dev`

---

# Glue Catalog

## **RAW Layer : JSON**
- **Crawler** : `nj-yta-glue-catalog-raw-json`
- **DataSource Crawling** : `s3://nj-yta-raw-apsouth1-dev/youtube/video_category_details_json/`
- **Database** : `nj-yta-db-raw`
- **IAM** : `nj-yta-raw-glue-s3-glueService`

## **RAW Layer : CSV**
- **Crawler** : `nj-yta-glue-catalog-raw-csv`
- **DataSource Crawling** : `s3://nj-yta-raw-apsouth1-dev/youtube/video_details/`
- **Database** : `nj-yta-db-raw`
- **Table name** : `video_details`
- **IAM** : `nj-yta-raw-glue-s3-glueService`



## **Clean Layer : JSON**
- **Crawler** : -
- **DataSource Crawling** : `s3://nj-yta-clean-apsouth1-dev/YT_video_category_details_json_normalized`
- **Database** : `nj-yta-db-clean`
- **Table name** : `video_category_details_json_normalized`

## **Clean Layer : CSV**
- **Crawler** : `nj-yta-glue-catalog-clean-csv`
- **DataSource Crawling** : `s3://nj-yta-clean-apsouth1-dev/YT_video_details_csv/`
- **Database** : `nj-yta-db-clean`
- **Table name** : `video_details_csv_clean`
- **IAM** : `nj-yta-raw-glue-s3-glueService`


---
# Lambda

- **Name :** `nj-yta-raw2clean-apsouth1-lambda-json2parquet`



---
# Glue Job for CSV Data
- **Name :**  `nj-yta-glue-etl-clean-csv2Parquet`
# Glue Job for Prsentation Layer
- **Name :**  `nj-yta-glue-etl-clean-inner-join`