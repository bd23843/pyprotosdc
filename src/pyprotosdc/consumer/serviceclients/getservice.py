from __future__ import annotations

import uuid
from typing import TYPE_CHECKING
from dataclasses import dataclass
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc, sdc_messages_pb2
from org.somda.protosdc.proto.model.biceps.abstractget_pb2 import AbstractGetMsg
from org.somda.protosdc.proto.model.biceps.getmdib_pb2 import GetMdibMsg
from org.somda.protosdc.proto.model.biceps.getmddescription_pb2 import GetMdDescriptionMsg
from org.somda.protosdc.proto.model.biceps.getmdstate_pb2 import GetMdStateMsg
from org.somda.protosdc.proto.model.biceps.getcontextstates_pb2 import GetContextStatesMsg
from sdc11073.mdib.mdibbase import MdibVersionGroup

from pyprotosdc.mapping.msgtypes_mappers import get_mdib_version_group
from pyprotosdc.mapping.mapping_helpers import get_p_attr
from pyprotosdc.actions import GetAction

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


@dataclass
class GetContextStatesResponseData:
    mdib_version_group: MdibVersionGroup
    p_response: sdc_messages_pb2.GetContextStatesResponse
    states: list[AbstractStateContainer]

@dataclass
class GetMdStateResponseData:
    mdib_version_group: MdibVersionGroup
    p_response: sdc_messages_pb2.GetMdStateResponse
    states: list[AbstractStateContainer]

@dataclass
class GetMdDescriptionResponseData:
    mdib_version_group: MdibVersionGroup
    p_response: sdc_messages_pb2.GetMdDescriptionResponse
    descriptors: list[AbstractDescriptorContainer]


class GetServiceWrapper():
    def __init__(self, channel, msg_reader: MessageReader):
        self._stub = sdc_services_pb2_grpc.GetServiceStub(channel)
        self._msg_reader = msg_reader

    def get_mdib(self) -> GetMdibResponseData:
        request = sdc_messages_pb2.GetMdibRequest()
        request.addressing.message_id = uuid.uuid4().urn
        request.addressing.action = GetAction.GetMdibRequest
        # make child fields payload and payload.abstract_get available
        payload = GetMdibMsg()
        abstract_get = AbstractGetMsg()
        payload.abstract_get.MergeFrom(abstract_get)
        request.payload.MergeFrom(payload)
        response = self._stub.GetMdib(request)

        mdib_version_group_msg = get_p_attr(response.payload.abstract_get_response, 'MdibVersionGroup')
        mdib_version_group = get_mdib_version_group(mdib_version_group_msg)
        descriptors, states = self._msg_reader.read_get_mdib_response(response)
        return GetMdibResponseData(mdib_version_group, response, descriptors, states)

    def get_md_description(self, handles: list[str] | None = None) -> GetMdDescriptionResponseData:
        request = sdc_messages_pb2.GetMdDescriptionRequest()
        request.addressing.message_id = uuid.uuid4().urn
        request.addressing.action = GetAction.GetMdDescriptionRequest

        # make child fields payload and payload.abstract_get available
        payload = GetMdDescriptionMsg()
        if handles:
            for handle in handles:
                payload.handle_ref.add().string = handle
        abstract_get = AbstractGetMsg()
        payload.abstract_get.MergeFrom(abstract_get)
        request.payload.MergeFrom(payload)

        response = self._stub.GetMdDescription(request)

        mdib_version_group_msg = get_p_attr(response.payload.abstract_get_response, 'MdibVersionGroup')
        mdib_version_group = get_mdib_version_group(mdib_version_group_msg)
        descriptors = self._msg_reader.read_md_description(response.payload.md_description)
        return GetMdDescriptionResponseData(mdib_version_group, response, descriptors)

    def get_md_state(self, handles: list[str] | None = None) -> GetMdStateResponseData:
        request = sdc_messages_pb2.GetMdStateRequest()
        request.addressing.message_id = uuid.uuid4().urn
        request.addressing.action = GetAction.GetMdStateRequest

        # make child fields payload and payload.abstract_get available
        payload = GetMdStateMsg()

        abstract_get = AbstractGetMsg()
        if handles:
            for handle in handles:
                payload.handle_ref.add().string = handle

        payload.abstract_get.MergeFrom(abstract_get)
        request.payload.MergeFrom(payload)

        response = self._stub.GetMdState(request)
        response.addressing.message_id = uuid.uuid4().urn
        response.addressing.action = 'GetMdib'
        mdib_version_group_msg = get_p_attr(response.payload.abstract_get_response, 'MdibVersionGroup')
        mdib_version_group = get_mdib_version_group(mdib_version_group_msg)
        mdib = None
        states = self._msg_reader.read_states(response.payload.md_state.state, mdib)
        return GetMdStateResponseData(mdib_version_group, response, states)

    def get_context_states(self, handles: list[str] | None = None) -> GetContextStatesResponseData:
        request = sdc_messages_pb2.GetContextStatesRequest()
        request.addressing.message_id = uuid.uuid4().urn
        request.addressing.action = GetAction.GetContextStateRequest

        # make child fields payload and payload.abstract_get available
        payload = GetContextStatesMsg()
        if handles:
            for handle in handles:
                payload.handle_ref.add().string = handle

        abstract_get = AbstractGetMsg()
        payload.abstract_get.MergeFrom(abstract_get)
        request.payload.MergeFrom(payload)

        response = self._stub.GetContextStates(request)
        mdib_version_group_msg = get_p_attr(response.payload.abstract_get_response, 'MdibVersionGroup')
        mdib_version_group = get_mdib_version_group(mdib_version_group_msg)
        mdib = None
        descriptors = self._msg_reader.read_states(response.payload.context_state, mdib)
        return GetContextStatesResponseData(mdib_version_group, response, descriptors)
