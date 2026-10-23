# MinIO configuration

This repository runs its own Airflow container and writes disclosure files to its own bucket, `big-data-capstone-disclosures`. Downloaded files use a temporary task directory inside the container. SSC records are uploaded and committed to PostgreSQL after each listing page, so a retry skips files already recorded in MinIO. The `data/ssc-checkpoint` directory is only used to recover PDFs downloaded before checkpointing was added; it is removed after a successful recovery run. The Stock_market project can keep its separate Airflow container and use a different bucket. Both buckets remain visible in the same MinIO Console when both projects connect to the same MinIO server.

Configure this project's ignored `airflow/.env`:

```dotenv
MINIO_ENDPOINT=http://host.docker.internal:9100
MINIO_BUCKET=big-data-capstone-disclosures
MINIO_ACCESS_KEY=your-existing-minio-access-key
MINIO_SECRET_KEY=your-existing-minio-secret-key
MINIO_REGION=us-east-1
MINIO_USE_SSL=false
```

The MinIO Console remains at `http://localhost:9001` in the current setup. The endpoint above is the S3 API port; it is not the Console URL. The collector creates its configured bucket if it does not exist and the MinIO credentials have permission to create buckets.

Copy `airflow/.env.example` to `airflow/.env` on a new checkout and enter that machine's PostgreSQL and MinIO settings. Never commit `airflow/.env`.

## Isolated project services

Compose in this repository runs a separate Airflow container and UI at `http://localhost:8081`. Its Airflow metadata database is `financial_airflow_meta`; the stock-market project should use its own metadata database. Both deployments can use the same MinIO server while assigning different bucket names.
