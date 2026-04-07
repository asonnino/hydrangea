# Copyright(C) Facebook, Inc. and its affiliates.
from json import load, JSONDecodeError


class SettingsError(Exception):
    pass


class Settings:
    def __init__(
        self,
        cloud_provider,
        base_port,
        repo_name,
        repo_url,
        branch,
        instance_type,
        # GCP-specific
        deploy_key_name=None,
        deploy_key_path=None,
        instance_key_name=None,
        instance_key_path=None,
        zones=None,
        # AWS-specific
        testbed=None,
        key_name=None,
        key_path=None,
        aws_regions=None,
    ):
        if cloud_provider not in ('aws', 'gcp'):
            raise SettingsError(f'Unknown cloud provider: {cloud_provider}')

        inputs_str = [repo_name, repo_url, branch, instance_type]
        ok = all(isinstance(x, str) for x in inputs_str)
        ok &= isinstance(base_port, int)
        if not ok:
            raise SettingsError('Invalid settings types')

        self.cloud_provider = cloud_provider
        self.base_port = base_port
        self.repo_name = repo_name
        self.repo_url = repo_url
        self.branch = branch
        self.instance_type = instance_type

        if cloud_provider == 'gcp':
            if not all([deploy_key_name, deploy_key_path,
                        instance_key_name, instance_key_path, zones]):
                raise SettingsError('GCP settings require deploy key, instance key, and zones')
            self.github_deploy_key_name = deploy_key_name
            self.github_deploy_key_path = deploy_key_path
            self.instance_key_name = instance_key_name
            self.instance_key_path = instance_key_path
            if isinstance(zones, list):
                self.zones = zones
            else:
                self.zones = [zones]

        elif cloud_provider == 'aws':
            if not all([testbed, key_name, key_path, aws_regions,
                        deploy_key_name, deploy_key_path]):
                raise SettingsError(
                    'AWS settings require testbed, key, github_deploy_key, and regions'
                )
            self.testbed = testbed
            self.github_deploy_key_name = deploy_key_name
            self.github_deploy_key_path = deploy_key_path
            self.instance_key_name = key_name
            self.instance_key_path = key_path
            if isinstance(aws_regions, list):
                self.aws_regions = aws_regions
            else:
                self.aws_regions = [aws_regions]

    @classmethod
    def load(cls, filename):
        try:
            with open(filename, 'r') as f:
                data = load(f)

            cloud_provider = data['cloud_provider']

            common = dict(
                cloud_provider=cloud_provider,
                base_port=data['port'],
                repo_name=data['repo']['name'],
                repo_url=data['repo']['url'],
                branch=data['repo']['branch'],
                instance_type=data['instances'].get('type')
                    or data['instances'].get('machine_type'),
            )

            if cloud_provider == 'gcp':
                return cls(
                    **common,
                    deploy_key_name=data['github_deploy_key']['name'],
                    deploy_key_path=data['github_deploy_key']['path'],
                    instance_key_name=data['instance_key']['name'],
                    instance_key_path=data['instance_key']['path'],
                    zones=data['instances']['zones'],
                )
            elif cloud_provider == 'aws':
                return cls(
                    **common,
                    testbed=data.get('testbed', 'hydrangea-bench'),
                    key_name=data['key']['name'],
                    key_path=data['key']['path'],
                    deploy_key_name=data['github_deploy_key']['name'],
                    deploy_key_path=data['github_deploy_key']['path'],
                    aws_regions=data['instances']['regions'],
                )
            else:
                raise SettingsError(f'Unknown cloud provider: {cloud_provider}')

        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')
