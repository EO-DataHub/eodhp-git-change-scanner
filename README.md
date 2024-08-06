# UKEODHP Git Change Scanner

Updates an S3 bucket from a GitHub repository

## Requirements
Ensure that configscanning is installed

## Running
``` bash
python3 -m configscanning.git_change_scanner https://github.com/UKEODHP/catalogue-data /path/to/staging/dir/catalogue-data --app-id-from app-id --app-private-key-from app-private-key --enable-scanner /path/to/eodhp/scanner/eodhp-scanner  --s3_bucket my_bucket --s3_prefix git-harvester --workspace workspace_name --harvester-id abc123 --config-scan --branch test_catalog --target my-target
```

# Install via makefile

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
