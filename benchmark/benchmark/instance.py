# Copyright(C) Facebook, Inc. and its affiliates.
from benchmark.settings import Settings, SettingsError
from benchmark.utils import BenchError


class InstanceManager:
    @classmethod
    def make(cls, settings_file='settings.json'):
        try:
            settings = Settings.load(settings_file)
        except SettingsError as e:
            raise BenchError('Failed to load settings', e)

        if settings.cloud_provider == 'aws':
            from benchmark.aws import AWSInstanceManager
            return AWSInstanceManager(settings)
        elif settings.cloud_provider == 'gcp':
            from benchmark.gcp import GCPInstanceManager
            return GCPInstanceManager(settings)
        else:
            raise BenchError(
                f'Unknown cloud provider: {settings.cloud_provider}'
            )
