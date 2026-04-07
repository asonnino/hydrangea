# Copyright(C) Facebook, Inc. and its affiliates.
from collections import OrderedDict
from time import sleep

from botocore.exceptions import ClientError
import boto3

from benchmark.utils import Print, BenchError, progress_bar


class AWSError(Exception):
    def __init__(self, error):
        assert isinstance(error, ClientError)
        self.message = error.response["Error"]["Message"]
        self.code = error.response["Error"]["Code"]
        super().__init__(self.message)


class AWSInstanceManager:
    INSTANCE_NAME = "hydrangea-bench"

    def __init__(self, settings):
        self.settings = settings
        self.clients = OrderedDict()
        for region in settings.aws_regions:
            self.clients[region] = boto3.client("ec2", region_name=region)

    def _get(self, state):
        assert isinstance(state, list)
        ids, ips = OrderedDict(), OrderedDict()
        for region, client in self.clients.items():
            try:
                response = client.describe_instances(
                    Filters=[
                        {"Name": "tag:Name", "Values": [self.settings.testbed]},
                        {"Name": "instance-state-name", "Values": state},
                    ]
                )
            except ClientError as e:
                raise AWSError(e)

            region_ids, region_ips = [], []
            for r in response["Reservations"]:
                for i in r["Instances"]:
                    region_ids += [i["InstanceId"]]
                    if "PublicIpAddress" in i:
                        region_ips += [i["PublicIpAddress"]]
            if region_ids:
                ids[region] = region_ids
            if region_ips:
                ips[region] = region_ips
        return ids, ips

    def _wait(self, state):
        assert isinstance(state, list)
        Print.info(f"Waiting for instances to become {state[0]}...")
        while True:
            sleep(10)
            ids, _ = self._get(state)
            total = sum(len(x) for x in ids.values())
            if total:
                break

    def _get_ami(self, client):
        response = client.describe_images(
            Filters=[
                {
                    "Name": "name",
                    "Values": [
                        "ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"
                    ],
                },
                {"Name": "owner-id", "Values": ["099720109477"]},  # Canonical
            ]
        )
        images = sorted(
            response["Images"], key=lambda x: x["CreationDate"], reverse=True
        )
        if not images:
            raise BenchError("No Ubuntu 22.04 AMI found", RuntimeError("AMI lookup returned empty"))
        return images[0]["ImageId"]

    def _create_security_group(self, client, region):
        sg_name = f"{self.settings.testbed}-sg"
        try:
            # Reuse existing security group if it already exists.
            response = client.describe_security_groups(
                Filters=[{"Name": "group-name", "Values": [sg_name]}]
            )
            if response["SecurityGroups"]:
                return response["SecurityGroups"][0]["GroupId"]

            response = client.create_security_group(
                GroupName=sg_name,
                Description=f"Security group for {self.settings.testbed}",
            )
            sg_id = response["GroupId"]
            base = self.settings.base_port
            client.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 22,
                        "ToPort": 22,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    },
                    {
                        "IpProtocol": "tcp",
                        "FromPort": base,
                        "ToPort": base + 500,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    },
                ],
            )
            return sg_id
        except ClientError as e:
            raise AWSError(e)

    def _delete_security_group(self, client, region):
        sg_name = f"{self.settings.testbed}-sg"
        try:
            response = client.describe_security_groups(
                Filters=[{"Name": "group-name", "Values": [sg_name]}]
            )
            for sg in response["SecurityGroups"]:
                client.delete_security_group(GroupId=sg["GroupId"])
        except ClientError as e:
            raise AWSError(e)

    def create_instances(self, instances):
        assert instances > 0
        try:
            for region, client in self.clients.items():
                Print.info(f"Creating {instances} instances in {region}...")
                sg_id = self._create_security_group(client, region)
                ami = self._get_ami(client)
                client.run_instances(
                    ImageId=ami,
                    InstanceType=self.settings.instance_type,
                    KeyName=self.settings.instance_key_name,
                    MaxCount=instances,
                    MinCount=instances,
                    SecurityGroupIds=[sg_id],
                    BlockDeviceMappings=[
                        {
                            "DeviceName": "/dev/sda1",
                            "Ebs": {"VolumeSize": 200, "VolumeType": "gp2"},
                        }
                    ],
                    TagSpecifications=[
                        {
                            "ResourceType": "instance",
                            "Tags": [{"Key": "Name", "Value": self.settings.testbed}],
                        }
                    ],
                )

            Print.info("Waiting for all instances to boot...")
            self._wait(["running"])
            total = instances * len(self.clients)
            Print.heading(f"Successfully created {total} new instances")
        except ClientError as e:
            raise BenchError("Failed to create AWS instances", AWSError(e))

    def delete_instances(self):
        try:
            ids, _ = self._get(["running", "stopped", "pending"])
            for region, client in self.clients.items():
                if region in ids:
                    Print.info(f"Terminating instances in {region}...")
                    client.terminate_instances(InstanceIds=ids[region])

            if ids:
                Print.info("Waiting for instances to terminate...")
                sleep(20)

            for region, client in self.clients.items():
                try:
                    self._delete_security_group(client, region)
                except AWSError:
                    Print.warn(f"Failed to delete security group in {region}")

            Print.heading("Testbed instances destroyed")
        except ClientError as e:
            raise BenchError("Failed to terminate instances", AWSError(e))

    def start_instances(self):
        try:
            ids, _ = self._get(["stopped"])
            for region, client in self.clients.items():
                if region in ids:
                    client.start_instances(InstanceIds=ids[region])
            if ids:
                self._wait(["running"])
            Print.heading(f"Instances started successfully")
        except ClientError as e:
            raise BenchError("Failed to start instances", AWSError(e))

    def stop_instances(self):
        try:
            ids, _ = self._get(["running"])
            for region, client in self.clients.items():
                if region in ids:
                    client.stop_instances(InstanceIds=ids[region])
            Print.heading("Instances stopped successfully")
        except ClientError as e:
            raise BenchError("Failed to stop instances", AWSError(e))

    def hosts(self, flat=False):
        try:
            _, ips = self._get(["running"])
            return [x for y in ips.values() for x in y] if flat else ips
        except ClientError as e:
            raise BenchError("Failed to gather instances IPs", AWSError(e))

    def print_info(self):
        hosts = self.hosts()
        key = self.settings.instance_key_path
        text = ""
        for region, ips in hosts.items():
            text += f"\n Region: {region.upper()}\n"
            for i, ip in enumerate(ips):
                new_line = "\n" if (i + 1) % 6 == 0 else ""
                text += f"{new_line} {i}\tssh -i {key} ubuntu@{ip}\n"
        print(
            "\n"
            "----------------------------------------------------------------\n"
            " INFO:\n"
            "----------------------------------------------------------------\n"
            f" Available machines: {sum(len(x) for x in hosts.values())}\n"
            f"{text}"
            "----------------------------------------------------------------\n"
        )
