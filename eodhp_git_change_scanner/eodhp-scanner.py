import json
import logging
import os
import urllib

import boto3
import botocore
import pulsar

logger = logging.getLogger(__name__)


def update_file(
    local_path: str,
    s3_path: str,
    s3_bucket_name: str,
    s3: boto3.resource,
) -> None:
    """Updates file in S3 from local directory"""
    logging.info(f"Updating {local_path} into {s3_bucket_name} {s3_path}")

    s3.Bucket(s3_bucket_name).upload_file(local_path, s3_path)


def delete_file(path: str, bucket_name: str, s3: boto3.resource) -> None:
    """Deletes any files no longer present"""
    logging.info(f"Deleting {path}")

    s3.Object(bucket_name, path).delete()


class Scanner:
    def __init__(self, **kwargs):
        self.added_files = []
        self.updated_files = []
        self.deleted_files = []
        self.s3 = None

        self.initialise_s3()

        dictionary = kwargs["kwargs"]
        self.harvester_id = dictionary.get("harvester-id", None)
        self.s3_bucket_name = dictionary["s3_bucket"]
        self.repo_name = urllib.parse.urlparse(dictionary["repourl"]).path.strip("/")
        self.branch = dictionary.get("branch", "main")
        self.workspace = dictionary.get("workspace_namespace", "workspace")
        self.local_folder = dictionary["dest"]
        self.target = dictionary.get("target", None)

        self.s3_prefix = "/".join(
            [dictionary["s3_prefix"], self.workspace, self.repo_name, self.branch]
        )

    def initialise_s3(self):
        if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
            session = boto3.session.Session(
                aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            )
            self.s3 = session.resource("s3")

        else:
            self.s3 = boto3.resource("s3")

    def finish(self):
        client = pulsar.Client(os.environ.get("PULSAR_URL"))
        producer = client.create_producer(topic="harvested", producer_name="git_change_scanner")

        try:
            message_information = {
                "id": self.harvester_id,
                "workspace": self.workspace,
                "repository": self.repo_name,
                "branch": self.branch,
                "bucket_name": self.s3_bucket_name,
                "added_keys": self.added_files,
                "updated_keys": self.updated_files,
                "deleted_keys": self.deleted_files,
                "source": "/",
                "target": self.target,
            }

            logging.info(message_information)

            msg = json.dumps(message_information)
            producer.send(msg.encode("utf-8"))
        except Exception:
            msg = "Harvester error"
            producer.send(msg.encode("utf-8"))
        finally:
            producer.close()
            client.close()
            logging.debug("Complete")

    def scan_file(self, file_name, data=None):
        local_path = f"{self.local_folder}/github.com/{self.repo_name}/{file_name}"
        s3_path = f"{self.s3_prefix}/{file_name}"

        if os.path.exists(local_path):
            try:  # s3 file exists and is updated
                self.s3.Object(self.s3_bucket_name, f"{self.s3_prefix}/{file_name}").load()
                update_file(local_path, s3_path, self.s3_bucket_name, self.s3)
                self.updated_files.append(f"{self.s3_prefix}/{file_name}")
            except botocore.exceptions.ClientError:  # s3 file doesn't exist and is added
                update_file(local_path, s3_path, self.s3_bucket_name, self.s3)
                self.added_files.append(f"{self.s3_prefix}/{file_name}")

        else:  # s3 file is deleted
            delete_file(s3_path, self.s3_bucket_name, self.s3)
            self.deleted_files.append(f"{self.s3_prefix}/{file_name}")
