# UKEODHP Git Change Scanner

Scans Git repositories and harvests new or updated files into the EODHP harvest pipeline.

## Requirements
Ensure that configscanning is installed

### Environment variables
- `AWS_ACCESS_KEY` - AWS access key ID (optional)
- `AWS_SECRET_ACCESS_KEY` - AWS secret access key (optional)
- `PULSAR_URL` - URL of pulsar service
- `TOPIC` - suffix for topic of pulsar messages

## Running
``` bash
python3 -m configscanning.git_change_scanner https://github.com/UKEODHP/catalogue-data /path/to/staging/dir/catalogue-data --app-id-from app-id --app-private-key-from app-private-key --enable-scanner /path/to/eodhp/scanner/eodhp-scanner  --s3_bucket my_bucket --s3_prefix git-harvester --workspace workspace_name --harvester-id abc123 --config-scan --branch test_catalog --target my-target
```

## Setup

```commandline
make setup
```

This will create a virtual environment called `venv`, build `requirements.txt` and
`requirements-dev.txt` from `pyproject.toml` if they're out of date, install the Python
and Node dependencies and install `pre-commit`.

It's safe and fast to run `make setup` repeatedly as it will only update these things if
they have changed.

After `make setup` you can run `pre-commit` to run pre-commit checks on staged changes and
`pre-commit run --all-files` to run them on all files. This replicates the linter checks that
run from GitHub actions.


## Pulsar Messages

### Outgoing Pulsar Messages (`harvested` topic)

After processing, the service sends a message to the `harvested` topic.

```json
{
  "id": "<unique_id>",
  "workspace": "<workspace_name>",
  "bucket_name": "<destination_s3_bucket>",
  "source": "<source_url_prefix>",
  "target": "<target_url_prefix>",
  "updated_keys": ["<list/of/transformed/keys>"],
  "deleted_keys": ["<list/of/deleted/keys>"],
  "added_keys": ["<list/of/newly/added/keys>"]
}
```

- `bucket_name`: S3 bucket where files are stored (may be different from the source).
- `updated_keys`, `deleted_keys`, `added_keys`: Refer to the updated, deleted, or added files in the output catalogue.
