import hashlib
import json
import logging
import os
import urllib
from pathlib import Path

import boto3
import botocore
import pulsar
import requests
from botocore.exceptions import ClientError
from html_sanitizer import Sanitizer

logger = logging.getLogger(__name__)


def update_file(
    local_path: str,
    s3_path: str,
    s3_bucket_name: str,
    s3: boto3.resource,
) -> None:
    """Updates file in S3 from local directory"""
    logging.info(f"Updating {local_path} into {s3_bucket_name} {s3_path}")

    # Determine content type based on file extension
    if local_path.endswith(".html"):
        content_type = "text/html"
    elif local_path.endswith(".txt"):
        content_type = "text/plain"
    else:
        content_type = "binary/octet-stream"  # Default for other file types

    s3.Bucket(s3_bucket_name).upload_file(
        local_path, s3_path, ExtraArgs={"ContentType": content_type}
    )


def update_licence_file(
    local_path: str,
    s3_path: str,
    s3_bucket_name: str,
    s3_client: boto3.client,
) -> None:
    """Updates file in S3 from local directory"""
    logging.info(f"Updating {local_path} into {s3_bucket_name} {s3_path}")

    # Determine content type based on file extension
    if local_path.endswith(".html"):
        content_type = "text/html"
        # Sanitize HTML content in memory
        with open(local_path, "r") as file:
            html_content = file.read()
        sanitizer = Sanitizer()
        result = sanitizer.sanitize(html_content)
    elif local_path.endswith(".txt"):
        content_type = "text/plain"
        with open(local_path, "r") as file:
            result = file.read()
    else:
        content_type = "binary/octet-stream"  # Default for other file types
        with open(local_path, "rb") as file:
            result = file.read()

    try:
        s3_client.head_object(Bucket=s3_bucket_name, Key=s3_path)
        logging.info(
            f"File {s3_path} already exists in S3 bucket {s3_bucket_name}. Skipping upload."
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            if content_type == "binary/octet-stream":
                s3_client.put_object(
                    Bucket=s3_bucket_name,
                    Key=s3_path,
                    Body=result,
                    ContentType=content_type,
                )
            else:
                s3_client.put_object(
                    Bucket=s3_bucket_name,
                    Key=s3_path,
                    Body=result.encode("utf-8"),
                    ContentType=content_type,
                )
            logging.info(f"Uploaded file {local_path} to S3 bucket {s3_bucket_name} at {s3_path}.")
        else:
            raise


def delete_file(path: str, bucket_name: str, s3: boto3.resource) -> None:
    """Deletes any files no longer present"""
    logging.info(f"Deleting {path}")

    s3.Object(bucket_name, path).delete()


class Scanner:
    def __init__(self, **kwargs):
        # Lists of STAC files
        self.added_files = []
        self.updated_files = []
        self.deleted_files = []

        # Lists of Annotations files
        self.added_annotations = []
        self.updated_annotations = []
        self.deleted_annotations = []

        # S3 Client
        self.s3 = None

        self.initialise_s3()

        args = kwargs["kwargs"]
        self.harvester_id = args.get("harvester_id", None)
        if "s3_bucket" in args:
            self.s3_bucket_name = args["s3_bucket"]
        else:
            self.s3_bucket_name = os.getenv("S3_BUCKET")
        if self.s3_bucket_name is None:
            raise KeyError("S3_BUCKET not set")
        self.s3_bucket_access_policy_name = os.getenv("S3_BUCKET_ACCESS_POLICY_NAME")
        if self.s3_bucket_access_policy_name is None:
            raise KeyError("S3_BUCKET_ACCESS_POLICY_NAME not set")
        self.repo_name = urllib.parse.urlparse(args["repourl"]).path.strip("/")
        self.branch = args.get("branch", "main")
        self.workspace = args.get("workspace_namespace", "workspace")
        self.local_folder = args["dest"]
        self.source = args.get("source", "/")
        self.target = args.get("target", None)
        self.hosted_zone = os.getenv("HOSTED_ZONE")
        self.spdx_bucket_name = os.getenv("SPDX_S3_BUCKET")
        self.s3_prefix = "/".join([args["s3_prefix"], self.workspace, self.repo_name, self.branch])

    def initialise_s3(self):
        if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
            session = boto3.session.Session(
                aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
                aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            )
            self.s3 = session.resource("s3")
            self.s3_client = session.client("s3")
        else:
            self.s3 = boto3.resource("s3")
            self.s3_client = boto3.client("s3")

    def send_message(self, client, producer, message, end=False):
        try:
            logging.info(message)
            msg = json.dumps(message)
            producer.send(msg.encode("utf-8"))
        except Exception:
            msg = "Harvester error"
            producer.send(msg.encode("utf-8"))
        finally:
            producer.close()
            if end:
                client.close()
                logging.debug("Complete")

    def finish(self):
        client = pulsar.Client(os.environ.get("PULSAR_URL"))
        if os.getenv("TOPIC"):
            identifier = "_" + os.getenv("TOPIC")
        else:
            identifier = ""

        # STAC Producer
        producer = client.create_producer(
            topic=f"harvested{identifier}", producer_name=f"git_change_scanner{identifier}"
        )

        # Annotations Producer
        producer_annotations = client.create_producer(
            topic=f"harvested-annotations{identifier}",
            producer_name=f"git_change_scanner_annotations{identifier}",
        )

        # Send messages to Pulsar
        if self.added_files or self.updated_files or self.deleted_files:
            logging.info("Sending STAC Data via Pulsar")
            # Construct STAC Pulsar message
            stac_message_information = {
                "id": self.harvester_id,
                "workspace": self.workspace,
                "repository": self.repo_name,
                "branch": self.branch,
                "bucket_name": self.s3_bucket_name,
                "added_keys": self.added_files,
                "updated_keys": self.updated_files,
                "deleted_keys": self.deleted_files,
                "source": self.source,
                "target": self.target,
            }
            self.send_message(client, producer, stac_message_information)

        if self.added_annotations or self.updated_annotations or self.deleted_annotations:
            logging.info("Sending Annotations Data via Pulsar")
            # Construct Annotations Pulsar message
            annotations_message_information = {
                "id": self.harvester_id,
                "workspace": self.workspace,
                "repository": self.repo_name,
                "branch": self.branch,
                "bucket_name": self.s3_bucket_name,
                "added_keys": self.added_annotations,
                "updated_keys": self.updated_annotations,
                "deleted_keys": self.deleted_annotations,
                "source": self.source,
                "target": self.target,
            }
            self.send_message(
                client, producer_annotations, annotations_message_information, end=True
            )

    def process_license_link(
        self,
        local_path: str,
        href: str,
        workspace: str,
        s3_bucket_name: str,
        s3: boto3.resource,
        hosted_zone: str,
    ) -> str:
        """Processes the license link and uploads the license file to S3 if necessary"""
        if href.startswith("http"):
            # Download the file from the URL
            response = requests.get(href)
            if response.status_code == 200:
                license_local_path = os.path.join("/tmp", os.path.basename(href))
                with open(license_local_path, "wb") as file:
                    file.write(response.content)
            else:
                logging.error(f"Failed to download file from {href}")
                return href
        else:
            license_local_path = os.path.normpath(os.path.join(os.path.dirname(local_path), href))

        if os.path.exists(license_local_path):
            # Compute a hash of the file content to ensure uniqueness
            with open(license_local_path, "rb") as file:
                file_content = file.read()
                file_hash = hashlib.md5(file_content).hexdigest()
            file_name = os.path.basename(href)
            license_s3_path = (
                f"api/catalogue/licences/{workspace}/{file_hash}-{file_name.replace('_', '-')}"
            )
            try:
                self.s3_client.head_object(Bucket=s3_bucket_name, Key=license_s3_path)
                logging.info(
                    f"File {license_s3_path} already exists in S3 bucket {s3_bucket_name}. Skipping upload."
                )
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] != "404":
                    raise
                update_licence_file(
                    license_local_path, license_s3_path, s3_bucket_name, self.s3_client
                )
            return f"https://{hosted_zone}/{license_s3_path}"
        return href

    def scan_file(self, file_name: Path, data=None):
        file_name = str(file_name)
        local_path = f"{self.local_folder}/github.com/{self.repo_name}/{file_name}"
        s3_path = f"{self.s3_prefix}/{file_name}"

        if os.path.exists(local_path):
            try:  # s3 file exists and is updated
                if file_name.endswith("-access_policy.json"):
                    self.s3.Object(self.s3_bucket_access_policy_name, file_name).load()
                    update_file(local_path, file_name, self.s3_bucket_access_policy_name, self.s3)
                else:
                    self.s3.Object(self.s3_bucket_name, s3_path).load()
                    if file_name.endswith("-annotations.json"):
                        self.updated_annotations.append(s3_path)
                    else:
                        self.updated_files.append(s3_path)
                        if local_path.endswith(".json"):
                            with open(local_path, "r") as file:
                                content = json.load(file)
                                if content.get("type") == "Collection":
                                    for link in content.get("links", []):
                                        if link.get("rel") == "license":
                                            new_href = self.process_license_link(
                                                local_path,
                                                link.get("href"),
                                                self.workspace,
                                                self.spdx_bucket_name,
                                                self.s3,
                                                self.hosted_zone,
                                            )
                                            link["href"] = new_href
                                    # Serialize the modified content to a JSON string
                                    temp_path = os.path.join("/tmp", local_path)
                                    with open(temp_path, "w") as updated_file:
                                        json.dump(content, updated_file, indent=2)
                                    local_path = temp_path
                                    print(f"Processed license links in {file_name}")
                    update_file(local_path, s3_path, self.s3_bucket_name, self.s3)

            except botocore.exceptions.ClientError:  # s3 file doesn't exist and is added
                if file_name.endswith("-access_policy.json"):
                    update_file(local_path, file_name, self.s3_bucket_access_policy_name, self.s3)
                else:
                    if file_name.endswith("-annotations.json"):
                        self.added_annotations.append(s3_path)
                    else:
                        self.added_files.append(s3_path)
                        if local_path.endswith(".json"):
                            with open(local_path, "r") as file:
                                content = json.load(file)
                                if content.get("type") == "Collection":
                                    for link in content.get("links", []):
                                        if link.get("rel") == "license":
                                            new_href = self.process_license_link(
                                                local_path,
                                                link.get("href"),
                                                self.workspace,
                                                self.spdx_bucket_name,
                                                self.s3,
                                                self.hosted_zone,
                                            )
                                            link["href"] = new_href
                                    # Serialize the modified content to a JSON string
                                    temp_path = os.path.join("/tmp", local_path)
                                    with open(temp_path, "w") as updated_file:
                                        json.dump(content, updated_file, indent=2)
                                    local_path = temp_path
                                    print(f"Processed license links in {file_name}")
                    update_file(local_path, s3_path, self.s3_bucket_name, self.s3)

        else:  # s3 file is deleted
            if file_name.endswith("-access_policy.json"):
                delete_file(file_name, self.s3_bucket_access_policy_name, self.s3)
            else:
                delete_file(s3_path, self.s3_bucket_name, self.s3)
                if file_name.endswith("-annotations.json"):
                    self.deleted_annotations.append(s3_path)
                else:
                    self.deleted_files.append(s3_path)

    # def process_collection_licenses(self, local_path: str, content: dict) -> None:
    #     """Processes license links in a Collection type file"""
    #     for link in content.get("links", []):
    #         if link.get("rel") == "license":
    #             href = link.get("href")
    #             if href:
    #                 new_href = self.process_license_link(
    #                     local_path,
    #                     href,
    #                     self.workspace,
    #                     self.spdx_bucket_name,
    #                     self.s3,
    #                     self.hosted_zone,
    #                 )
    #                 link["href"] = new_href
    #     with open(local_path, "w") as updated_file:
    #         json.dump(content, updated_file, indent=2)
