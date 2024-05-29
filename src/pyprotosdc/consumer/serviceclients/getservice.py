from __future__ import annotations
from typing import TYPE_CHECKING
from dataclasses import dataclass
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc, sdc_messages_pb2

from sdc11073.mdib.mdibbase import MdibVersionGroup

from pyprotosdc.mapping.msgtypes_mappers import get_mdib_version_group
from pyprotosdc.mapping.mapping_helpers import get_p_attr


if TYPE_CHECKING:
    from pyprotosdc.msgreader import MessageReader
    from sdc11073.mdib.descriptorcontainers import AbstractDescriptorContainer
    from sdc11073.mdib.statecontainers import AbstractStateContainer


@dataclass
class GetMdibResponseData:
    mdib_version_group: MdibVersionGroup
    p_response: sdc_messages_pb2.GetMdibResponse
    descriptors: list[AbstractDescriptorContainer]
    states: list[AbstractStateContainer]

class GetService_Wrapper():
    def __init__(self, channel, msg_reader: MessageReader):
        # super().__init__(channel)
        self._stub = sdc_services_pb2_grpc.GetServiceStub(channel)
        self._msg_reader = msg_reader

    def get_mdib(self) -> GetMdibResponseData:
        request = sdc_messages_pb2.GetMdibRequest()
        response = self._stub.GetMdib(request)
        mdib_version_group_msg = get_p_attr(response.payload.abstract_get_response, 'MdibVersionGroup')
        mdib_version_group = get_mdib_version_group(mdib_version_group_msg)
        descriptors, states = self._msg_reader.read_get_mdib_response(response)
        return GetMdibResponseData(mdib_version_group, response, descriptors, states)
