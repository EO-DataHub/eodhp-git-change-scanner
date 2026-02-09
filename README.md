# EODHP Git Change Scanner

Scans Git repositories and harvests new or updated files into the EODHP harvest pipeline.

## Requirements

- [uv](https://docs.astral.sh/uv/) (for dependency management)

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

This will install dependencies with `uv sync` and set up `pre-commit` hooks.

It's safe and fast to run `make setup` repeatedly.

## Development

| Command | Description |
|---------|-------------|
| `make setup` | Install dependencies and pre-commit hooks |
| `make test` | Run tests in watch mode |
| `make testonce` | Run tests once |
| `make check` | Run all linting and type checks (ruff, pyright, validate-pyproject) |
| `make format` | Auto-fix lint issues and format code |
| `make pre-commit` | Run pre-commit checks on staged changes |
| `make pre-commit-all` | Run pre-commit checks on all files |


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
