from __future__ import annotations

import logging
import traceback
import uuid
from typing import TYPE_CHECKING

from org.somda.protosdc.proto.model.metadata import metadata_services_pb2_grpc
from org.somda.protosdc.proto.model.metadata.metadata_messages_pb2 import GetMetadataRequest, GetMetadataResponse

from pyprotosdc.mapping.generic import generic_to_p

if TYPE_CHECKING:
    from pyprotosdc.provider.provider import GSdcDevice

def localized_string_to_p(pm_src, p_dest):
    p_dest.value = pm_src.text
    if pm_src.lang:
        p_dest.locale = pm_src.lang

class MetadataService(metadata_services_pb2_grpc.MetadataServiceServicer):

    def __init__(self, provider: GSdcDevice):
        super().__init__()
        self._provider = provider
        self._logger = logging.getLogger('sdc.grpc.dev.MetaDataService')

    def GetMetadata(self, request: GetMetadataRequest, context) -> GetMetadataResponse:
        response = GetMetadataResponse()
        response.addressing.action = 'org.somda.protosdc.metadata.action.GetMetadataResponse'
        response.addressing.message_id = uuid.uuid4().urn
        response.addressing.relates_id.value = request.addressing.message_id

        addresses = self._provider.get_xaddrs()

        response.endpoint.endpoint_identifier = self._provider.epr
        for address in addresses:
            uri = response.endpoint.physical_address.add()
            uri.value = address
        for scope in self._provider.mk_scopes():
            uri = response.endpoint.scope.add()
            uri.value = scope

        response.metadata.firmware_version.value = self._provider.this_device.FirmwareVersion
        for text in self._provider.this_device.FriendlyName:
            dest = response.metadata.friendly_name.add()
            localized_string_to_p(text, dest)
        response.metadata.serial_number.value = self._provider.this_device.SerialNumber
        for text in self._provider.this_model.Manufacturer:
            dest = response.metadata.manufacturer.add()
            localized_string_to_p(text, dest)
            # response.metadata.manufacturer.append(generic_to_p(text))
        response.metadata.manufacturer_url.value = self._provider.this_model.ManufacturerUrl
        for text in self._provider.this_model.ModelName:
            dest = response.metadata.model_name.add()
            localized_string_to_p(text, dest)
            # response.metadata.model_name.append(generic_to_p(text))
        response.metadata.model_url.value = self._provider.this_model.ModelUrl

        service = response.service.add()
        service.type.append('org.somda.protosdc.service.metadata.Metadata')
        service.type.append('org.somda.protosdc.service.Get')
        service.type.append('org.somda.protosdc.service.Set')
        service.type.append('org.somda.protosdc.service.MdibReporting')

        for address in addresses:
            uri = service.physical_address.add()
            uri.value = address
        service.service_identifier = 'pyprotosdc provider services'

        return response
