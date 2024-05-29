from __future__ import annotations
from typing import TYPE_CHECKING
import weakref
from dataclasses import dataclass
from concurrent.futures import Future
from threading import Lock
from sdc11073.xml_types.pm_types import InstanceIdentifier
from sdc11073 import loghelper
from sdc11073.consumer.operations import OperationResult
from sdc11073.xml_types import msg_types
from ..mapping.basic_mappers import enum_from_p
from ..mapping.mapping_helpers import get_p_attr
from ..mapping.generic import generic_from_p
from org.somda.protosdc.proto.model.biceps.abstractsetresponse_pb2 import AbstractSetResponseMsg

if TYPE_CHECKING:
    from org.somda.protosdc.proto.model.biceps.operationinvokedreport_pb2 import OperationInvokedReportMsg


@dataclass
class OperationResult:
    """OperationResult is the result of a Set operation.

    Usually only the result is relevant, but for testing all intermediate data is also available.
    """

    InvocationInfo: InvocationInfo
    InvocationSource: InstanceIdentifier | None
    OperationHandleRef: str | None
    OperationTarget: str | None

    set_response: AbstractSetResponseMsg
    # report_parts: list[
    #     msg_types.OperationInvokedReportPart]  # contains data of all OperationInvokedReportPart for operation

class GOperationsManager(object):
    nonFinalOperationStates = (msg_types.InvocationState.WAIT, msg_types.InvocationState.START)
    def __init__(self, log_prefix):
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.client.op_mgr', log_prefix)
        self._transactions = {}
        self._transactions_lock = Lock()

    def watch_operation(self, response):
        fut = Future()
        invocation_info = response.payload.abstract_set_response.invocation_info
        transaction_id = invocation_info.transaction_id.unsigned_int
        self._logger.debug('watch_operation: fut id = {}, transaction_id = {}', id(fut), transaction_id)
        #invocation_state = invocation_info.invocation_state.enum_value
        invocation_state = enum_from_p(invocation_info, 'invocation_state', msg_types.InvocationState)
        with self._transactions_lock:
            if invocation_state in self.nonFinalOperationStates:
                self._transactions[int(transaction_id)] = weakref.ref(fut)
                self._logger.info('watch_operation: transactionId {} registered, state={}', transaction_id, invocation_state)
            else:
                self._logger.info('watch_operation: transactionId {} finished, state={}', transaction_id, invocation_state)
                info = msg_types.InvocationInfo()
                generic_from_p(invocation_info, info)

                result = OperationResult(info, None, None, None, response)
                # errors = [] # ToDo: set
                # error = '' if len(errors) == 0 else str(errors[0])
                # error_msgs = []
                # error_msg = '' if len(error_msgs) == 0 else str(error_msgs[0])

                self._logger.debug('Result of Operation: {}',  result)
                fut.set_result(result)

        return fut

    # def on_operation_invoked_report(self, transaction_id, invocation_info):
    #     self._logger.debug('{}on_operation_invoked_report: got transactionId {} state {}',
    #                        self.log_prefix, transaction_id, invocation_info.state)
    #     if invocation_info.state in self.nonFinalOperationStates:
    #         self._logger.debug('nonFinal state detected, ignoring message...')
    #         return
    #     with self._transactions_lock:
    #         future_ref = self._transactions.pop(int(transaction_id), None)
    #     if future_ref is None:
    #         # this was not my transaction
    #         self._logger.debug('transactionId {} is not registered!', transaction_id)
    #         return
    #     future_obj = future_ref()
    #     if future_obj is None:
    #         # client gave up.
    #         self._logger.debug('transactionId {} given up', transaction_id)
    #         return
    #     else:
    #         print(f'on_operation_invoked_report: fut id = {id(future_obj)}')
    #         if invocation_info.state == InvocationState.FAILED:
    #             self._logger.warn('transaction Id {} finished with error: error={}, error-message={}',
    #                               transaction_id, invocation_info.error, invocation_info.error_messages)
    #         else:
    #             self._logger.info('transaction Id {} ok', transaction_id)
    #         future_obj.set_result(invocation_info)

    def on_operation_invoked_report(self, operation_invoked: OperationInvokedReportMsg):
        for report_part in operation_invoked.report_part:
            transaction_id = report_part.invocation_info.transaction_id.unsigned_int
            invocation_state = enum_from_p(report_part.invocation_info, 'invocation_state', msg_types.InvocationState)
            if invocation_state in self.nonFinalOperationStates:
                self._logger.debug('nonFinal state detected, ignoring report part...')
                continue

            error = enum_from_p(report_part.invocation_info, 'invocation_error', msg_types.InvocationError)
            errormessages = []

            with self._transactions_lock:
                future_ref = self._transactions.pop(int(transaction_id), None)
            if future_ref is None:
                # this was not my transaction
                self._logger.debug('transactionId {} is not registered!', transaction_id)
                continue

            future_obj = future_ref()
            if future_obj is None:
                # client gave up.
                self._logger.debug('transactionId {} given up', transaction_id)
                continue
            else:
                print(f'on_operation_invoked_report: fut id = {id(future_obj)}')
                info = msg_types.InvocationInfo()
                generic_from_p(report_part.invocation_info, info)
                src = generic_from_p(report_part.invocation_source)
                op_handle = get_p_attr(report_part, 'OperationHandleRef').string
                op_target_handle = get_p_attr(report_part, 'OperationTarget').string

                op_result = OperationResult(info, src, op_handle, op_target_handle, operation_invoked)

                if info.InvocationState == msg_types.InvocationState.FAILED:
                    self._logger.warn('transaction Id {} finished with error: error={}, error-message={}',
                                      transaction_id, info.InvocationError.error, info.InvocationErrorMessage)
                else:
                    self._logger.info('transaction Id {} ok', transaction_id)
                future_obj.set_result(op_result)
