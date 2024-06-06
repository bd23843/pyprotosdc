from __future__ import annotations

import logging
import traceback
import uuid
from typing import TYPE_CHECKING

from org.somda.protosdc.proto.model.metadata import metadata_services_pb2_grpc
from org.somda.protosdc.proto.model.metadata.metadata_messages_pb2 import GetMetadataRequest, GetMetadataResponse


class MetadataServiceWrapper:
    """Consumer-side implementation of MetadataService"""
    # operation_invoked_report = observableproperties.ObservableProperty()
    def __init__(self, channel):
        self._logger = logging.getLogger('sdc.grpc.cl.metadata')
        self._stub = metadata_services_pb2_grpc.MetadataServiceStub(channel)

    def get_metadata(self) -> GetMetadataResponse:
        request = GetMetadataRequest()
        request.addressing.message_id = uuid.uuid4().urn
        request.addressing.action = 'org.somda.protosdc.metadata.action.GetMetadata'
        response = self._stub.GetMetadata(request)
        return response
