# Copyright Amazon.com Inc. or its affiliates.
# SPDX-License-Identifier: MIT-0

"""
The Initial Commit main that is called when ADF is installed to commit the
initial bootstrap repository content.
"""

import os
import logging
import boto3
import jinja2
import shutil
import fnmatch
from pathlib import Path
from dataclasses import dataclass, fields
from typing import Mapping, Optional, Union, List, Dict, Any, Tuple

from cfn_custom_resource import (  # pylint: disable=unused-import
    lambda_handler,
    create,
    update,
    delete,
)

# pylint: disable=invalid-name

PhysicalResourceId = str
Data = Mapping[str, str]

HERE = Path(__file__).parent
S3_CLIENT = boto3.client("s3")
REWRITE_PATHS: Dict[str, str] = {
    "bootstrap_repository/adf-bootstrap/example-global-iam.yml": (
        "adf-bootstrap/global-iam.yml"
    ),
    "bootstrap_repository/adf-bootstrap/deployment/example-global-iam.yml": (
        "adf-bootstrap/deployment/global-iam.yml"
    ),
    "adf.yml.j2": "adf-accounts/adf.yml",
    "adfconfig.yml.j2": "adfconfig.yml",
}
EXECUTABLE_FILES: List[str] = [
    "adf-build/shared/helpers/package_transform.sh",
    "adf-build/shared/helpers/retrieve_organization_accounts.py",
    "adf-build/shared/helpers/sync_to_s3.py",
    "adf-build/shared/helpers/sts.sh",
    "adf-build/shared/helpers/terraform/adf_terraform.sh",
    "adf-build/shared/helpers/terraform/install_terraform.sh",
]

ADF_LOG_LEVEL = os.environ.get("ADF_LOG_LEVEL", "INFO")
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(ADF_LOG_LEVEL)

@dataclass
class CustomResourceProperties:
    # pylint: disable=too-many-instance-attributes
    ServiceToken: str
    DirectoryName: str
    Version: str
    BucketName: str
    ObjectKey: str
    CrossAccountAccessRole: Optional[str] = None
    DeploymentAccountRegion: Optional[str] = None
    ExistingAccountId: Optional[str] = None
    DeploymentAccountFullName: Optional[str] = None
    DeploymentAccountEmailAddress: Optional[str] = None
    DeploymentAccountAlias: Optional[str] = None
    TargetRegions: Optional[List[str]] = None
    NotificationEndpoint: Optional[str] = None
    NotificationEndpointType: Optional[str] = None
    ProtectedOUs: Optional[List[str]] = None
    SourceType: Optional[str] = None

    def __post_init__(self):
        if self.NotificationEndpoint:
            self.NotificationEndpointType = (
                "email"
                if self.NotificationEndpoint.find("@") > 0
                else "slack"
            )


def to_dict(datacls_or_dict: Any) -> dict:
    if isinstance(datacls_or_dict, CustomResourceProperties):
        return datacls_or_dict.__dict__
    return datacls_or_dict

@dataclass
class Event:
    RequestType: str
    ServiceToken: str
    ResponseURL: str
    StackId: str
    RequestId: str
    ResourceType: str
    LogicalResourceId: str
    ResourceProperties: CustomResourceProperties

    def __post_init__(self):
        # Used to filter out any properties that this class does not know about
        custom_resource_fields = list(map(
            lambda a: a.name,
            fields(CustomResourceProperties),
        ))
        self.ResourceProperties = CustomResourceProperties(
            **{
                key: value for key, value in to_dict(
                    self.ResourceProperties
                ).items()
                if key in custom_resource_fields
            }
        )

@dataclass
class CreateEvent(Event):
    pass


@dataclass
class UpdateEvent(Event):
    PhysicalResourceId: str
    OldResourceProperties: CustomResourceProperties

    def __post_init__(self):
        super().__post_init__()
        custom_resource_fields = list(map(
            lambda a: a.name,
            fields(CustomResourceProperties),
        ))
        self.OldResourceProperties = CustomResourceProperties(
            **{
                key: value for key, value in to_dict(
                    self.OldResourceProperties
                ).items()
                if key in custom_resource_fields
            }
        )

def s3_object_exists(bucket_name, object_key):
    """
    Check if an object exists in an S3 bucket.

    :param bucket_name: The name of the bucket to check.
    :param object_key: The key of the object to check.
    :return: Tuple of (object_exists, create_object)
    """
    # First check if the bucket exists
    try:
        S3_CLIENT.head_bucket(Bucket=bucket_name)
        LOGGER.info(f"Bucket {bucket_name} exists")
    except S3_CLIENT.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            LOGGER.warning(f"Bucket {bucket_name} does not exist")
            return (False, False)
        elif e.response['Error']['Code'] == '403':
            LOGGER.warning(f"Access denied to bucket {bucket_name}")
            # Bucket exists but we don't have access to it
            return (False, False)
        else:
            LOGGER.error(f"Error checking bucket {bucket_name}: {str(e)}")
            return (False, False)
    try:
        S3_CLIENT.head_object(
            Bucket=bucket_name,
            Key=object_key
        )
        LOGGER.info(f"Object {object_key} exists in bucket {bucket_name}")
        return (True, False)
    
    except S3_CLIENT.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            # Object does not exist
            LOGGER.info(
                "S3 object %s in bucket %s does not exist. "
                "Defaulting to creating the object instead.",
                object_key,
                bucket_name,
            )
            return (False,True)
        else:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            LOGGER.error(f"Error checking S3 object: {error_code} - {error_message}")
            return (False,False)

def create_adf_config_file(
    props: CustomResourceProperties,
    input_file_name: str,
    output_file_name: str,
) -> None:
    template = HERE / input_file_name
    adf_config = (
        jinja2.Template(template.read_text(), undefined=jinja2.StrictUndefined, autoescape=True)
        .render(vars(props))
        .encode()
    )

    with open(output_file_name, mode="wb") as file:
        file.write(adf_config)
    LOGGER.debug(f"Successfully created adf config in {output_file_name}")


def copy_directory_to_tmp(
    source_dir: str, 
    tmp_dir_name: Optional[str] = None,
    exclude_patterns: Optional[List[str]] = None,
) -> str:
    """
    Copy a directory to the /tmp directory in AWS Lambda.
    
    Args:
        source_dir (str): Path to the source directory to copy
        tmp_dir_name (str, optional): Name for the directory in /tmp. 
                                     If None, uses the basename of source_dir 
        exclude_patterns (List[str], optional): List of glob patterns to exclude       
    Returns:
        str: Path to the copied directory in /tmp
    """
    
    # Determine target directory name
    if tmp_dir_name is None:
        tmp_dir_name = os.path.basename(os.path.normpath(source_dir))
    
    # Create full target path
    target_dir = os.path.join('/tmp', tmp_dir_name)
    
    # Remove target directory if it already exists
    if os.path.exists(target_dir):
        LOGGER.info(f"Target directory already exists, removing: {target_dir}")
        shutil.rmtree(target_dir)

    if not exclude_patterns:
        # Use fast copy method if no exclusions or metadata preservation needed
        LOGGER.info(f"Copying directory {source_dir} to {target_dir}")
        shutil.copytree(source_dir, target_dir)
    else:
        # Custom copy with exclusions
        LOGGER.info(f"Copying directory with exclusions: {source_dir} to {target_dir}")
        os.makedirs(target_dir, exist_ok=True)
        
        files_copied = 0
        files_excluded = 0
        
        for root, dirs, files in os.walk(source_dir):
            # Calculate relative path from source_dir
            rel_path = os.path.relpath(root, source_dir)
            target_root = os.path.join(target_dir, rel_path) if rel_path != '.' else target_dir
            
            # Create directories
            os.makedirs(target_root, exist_ok=True)
            
            # Copy files
            for file in files:
                src_file = os.path.join(root, file)
                # Get the relative path for pattern matching
                rel_file_path = os.path.join(rel_path, file) if rel_path != '.' else file
                
                # Check if file should be excluded
                should_exclude = False
                for pattern in exclude_patterns:
                    if fnmatch.fnmatch(rel_file_path, pattern):
                        should_exclude = True
                        files_excluded += 1
                        LOGGER.debug(f"Excluding file: {rel_file_path} (matches pattern {pattern})")
                        break
                
                if not should_exclude:
                    dst_file = os.path.join(target_root, file)
                    shutil.copy2(src_file, dst_file)
                    files_copied += 1
        LOGGER.info(f"Copied {files_copied} files, excluded {files_excluded} files")
    LOGGER.info(f"Successfully copied directory to {target_dir}")
    return target_dir


def generate_s3_source_object(event, directory, bucket_name, object_key):
    # pylint: disable=too-many-locals
    directory_path = HERE / directory
    directory_tmp_path = copy_directory_to_tmp(directory_path, exclude_patterns=EXECUTABLE_FILES)

    if directory == "bootstrap_repository":
        adfconfig_j2_name = "adfconfig.yml.j2"
        adfconfig_file_ab_path = REWRITE_PATHS.get(adfconfig_j2_name)
        create_adf_config_file(
            event.ResourceProperties,
            adfconfig_j2_name,
            f"{directory_tmp_path}/{adfconfig_file_ab_path}",
        )
        global_iam_j2_name = "bootstrap_repository/adf-bootstrap/example-global-iam.yml"
        global_iam_ab_path = REWRITE_PATHS.get(global_iam_j2_name)
        create_adf_config_file(
            event.ResourceProperties,
            global_iam_j2_name,
            f"{directory_tmp_path}/{global_iam_ab_path}",
        )
        deploy_global_iam_j2_name = "bootstrap_repository/adf-bootstrap/deployment/example-global-iam.yml"
        deploy_global_iam_ab_path = REWRITE_PATHS.get(deploy_global_iam_j2_name)        
        create_adf_config_file(
            event.ResourceProperties,
            deploy_global_iam_j2_name,
            f"{directory_tmp_path}/{deploy_global_iam_ab_path}",
        )

        create_deployment_account = (
            event.ResourceProperties.DeploymentAccountFullName
            and event.ResourceProperties.DeploymentAccountEmailAddress
        )

        if create_deployment_account:
            adf_j2_name = "adf.yml.j2"
            adf_ab_path = REWRITE_PATHS.get(adf_j2_name)
            create_adf_config_file(
                event.ResourceProperties,
                adf_j2_name,
                f"{directory_tmp_path}/{adf_ab_path}",
            )
        # Zip the folder
        target_zip_file_path_prefix = f"/tmp/{object_key.split('.')[0]}"
        shutil.make_archive(target_zip_file_path_prefix, 'zip', directory_tmp_path)
        LOGGER.info(f"suceessfully zip the {directory_tmp_path}")
        S3_CLIENT.upload_file(f"{target_zip_file_path_prefix}.zip", bucket_name, object_key)
        LOGGER.info(f"suceessfully upload {object_key} to the s3 bucket {bucket_name}.")



@create()
def create_(
    event: Mapping[str, Any],
    _context: Any,
) -> Tuple[Union[None, PhysicalResourceId], Data]:
    create_event = CreateEvent(**event)
    directory = create_event.ResourceProperties.DirectoryName
    bucket_name = create_event.ResourceProperties.BucketName
    object_key = create_event.ResourceProperties.ObjectKey

    object_exists, create_object = s3_object_exists(bucket_name, object_key)
    if object_exists:
        LOGGER.info(f"object {object_key} already exits in the bucket {bucket_name}.")
    if create_object:
        generate_s3_source_object(
            event, 
            directory, 
            bucket_name, 
            object_key
        )
        return object_key, {}

    return event.get("PhysicalResourceId"), {}


@update()
def update_(
    event: Mapping[str, Any],
    _context: Any,
) -> Tuple[PhysicalResourceId, Data]:
    update_event = UpdateEvent(**event)
    directory = update_event.ResourceProperties.DirectoryName
    bucket_name = update_event.ResourceProperties.BucketName
    object_key = update_event.ResourceProperties.ObjectKey

    object_exists, create_object = s3_object_exists(bucket_name, object_key)
    if object_exists:
        LOGGER.info(f"object {object_key} already exits in the bucket {bucket_name}.")
    if create_object:
        generate_s3_source_object(
            event, 
            directory, 
            bucket_name, 
            object_key
        )

    return event.get("PhysicalResourceId"), {}


@delete()
def delete_(_event, _context):
    pass




