# UKEODHP Git Change Scanner

Updates an S3 bucket from a GitHub repository

## Requirements
Ensure that configscanning is installed

## Running
``` bash
python3 -m configscanning.git_change_scanner https://github.com/UKEODHP/catalogue-data /path/to/staging/dir/catalogue-data --app-id-from app-id --app-private-key-from app-private-key --enable-scanner /path/to/eodhp/scanner/eodhp-scanner  --s3_bucket my_bucket --s3_prefix git-harvester --workspace workspace_name --harvester-id abc123 --config-scan --branch test_catalog --target my-target
```
