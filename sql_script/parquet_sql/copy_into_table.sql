COPY INTO par_data
FROM @par_stage/TSLA.parquet
FILE_FORMAT = (TYPE=PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT';