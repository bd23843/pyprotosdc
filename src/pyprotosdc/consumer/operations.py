from __future__ import annotations
from typing import TYPE_CHECKING
import weakref
from collections import deque
from dataclasses import dataclass
from concurrent.futures import Future
from threading import Lock
from sdc11073.xml_types.pm_types import InstanceIdentifier
from sdc11073 import loghelper
from sdc11073.xml_types import msg_types
from sdc11073.etc import short_filter_string
from ..mapping.basic_mappers import enum_from_p
from ..mapping.mapping_helpers import get_p_attr
from ..mapping.generic import generic_from_p

if TYPE_CHECKING:
    from org.somda.protosdc.proto.model.biceps.operationinvokedreport_pb2 import OperationInvokedReportMsg
    from pyprotosdc.mapping.msgtypes_mappers import AnySetServiceResponse



@dataclass
class OperationResult:
    """OperationResult is the result of a Set operation.

    Usually only the result is relevant, but for testing all intermediate data is also available.
    """

    InvocationInfo: msg_types.InvocationInfo
    InvocationSource: InstanceIdentifier | None
    OperationHandleRef: str | None
    OperationTarget: str | None

    set_response: AnySetServiceResponse
    report_parts: list[OperationInvokedReportMsg.ReportPartMsg]  # data of all OperationInvokedReportPart for operation


@dataclass
class _OperationData:
    """collect all progress data of a transaction."""

    future_ref: weakref.ref[Future]
    set_response: AnySetServiceResponse
    report_parts: list[OperationInvokedReportMsg.ReportPartMsg]  # data of all OperationInvokedReportPart for operation


class GOperationsManager(object):
    nonFinalOperationStates = (msg_types.InvocationState.WAIT, msg_types.InvocationState.START)
    def __init__(self, log_prefix):
        self.log_prefix = log_prefix
        self._logger = loghelper.get_logger_adapter('sdc.client.op_mgr', log_prefix)
        self._transactions: dict[int, _OperationData] = {}
        self._transactions_lock = Lock()
        self._last_operation_invoked_reports: deque[OperationInvokedReportMsg.ReportPartMsg] = deque(maxlen=50)

    def watch_operation(self, response: AnySetServiceResponse) -> Future:
        """Wait for a final operation result and fill response with data.
        Returns a Future object. Its result is a OperationResult."""
        fut = Future()
        invocation_info = response.payload.abstract_set_response.invocation_info
        transaction_id = invocation_info.transaction_id.unsigned_int
        short_action = short_filter_string([response.addressing.action])
        invocation_state = enum_from_p(invocation_info, 'invocation_state', msg_types.InvocationState)

        if invocation_state == msg_types.InvocationState.FAILED:
            # do not wait for OperationInvokedReport, set result immediately
            # Todo: should we wait?
            info = msg_types.InvocationInfo()
            generic_from_p(invocation_info, info)
            error_text = ', '.join([msg.text for msg in info.InvocationErrorMessage])
            self._logger.warn('operation failed: transaction_id = {}, invocation state = {}, action = {}, error msg = {}',
                              transaction_id, invocation_state, short_action, error_text)

            op_result = OperationResult(info, None, None, None, response, [])
            fut.set_result(op_result)
        else:
            self._logger.info('watch_operation: transaction_id = {}, invocation state = {}, action = {}',
                              transaction_id, invocation_state, short_action)
            with self._transactions_lock:
                self._transactions[int(transaction_id)] = _OperationData(weakref.ref(fut), response, [])
        return fut

    def on_operation_invoked_report(self, operation_invoked: OperationInvokedReportMsg):
        """Process OperationInvokedReportMsg.

        Associate each report part to corresponding _OperationData.
        if  the report part contains a final invocation state, set the result of the Future object in
        corresponding _OperationData. That makes invocation result available to the caller of an operation."""
        for report_part in operation_invoked.report_part:
            transaction_id = report_part.invocation_info.transaction_id.unsigned_int
            invocation_state = enum_from_p(report_part.invocation_info,
                                           'invocation_state',
                                           msg_types.InvocationState)
            if transaction_id in self._transactions:
                # keep report part so that it can later be added to OperationResult
                self._transactions[transaction_id].report_parts.append(report_part)
            else:
                continue  # this is not one of our operations

            if invocation_state in self.nonFinalOperationStates:
                self._logger.info('transaction id %d: state = %s, still waiting for a final state...',
                                   transaction_id, invocation_state)
                continue

            with self._transactions_lock:
                operation_data = self._transactions.pop(int(transaction_id), None)
            if operation_data is None:
                # this was not my transaction
                self._logger.debug('transactionId {} is not registered!', transaction_id)
                continue

            future_obj = operation_data.future_ref()
            if future_obj is None:
                # client gave up.
                self._logger.info('transactionId {} given up', transaction_id)
                continue
            else:
                self._logger.info('final state %s detected for transaction %d', invocation_state, transaction_id)
                info = msg_types.InvocationInfo()
                generic_from_p(report_part.invocation_info, info)
                src = generic_from_p(report_part.invocation_source)
                op_handle = get_p_attr(report_part, 'OperationHandleRef').string
                op_target_handle = get_p_attr(report_part, 'OperationTarget').string

                op_result = OperationResult(info, src,
                                            op_handle,
                                            op_target_handle,
                                            operation_invoked,
                                            operation_data.report_parts)

                if info.InvocationState == msg_types.InvocationState.FAILED:
                    self._logger.warn('transaction Id {} finished with error: error={}, error-message={}',
                                      transaction_id, info.InvocationError, info.InvocationErrorMessage)
                else:
                    self._logger.info('transaction Id {} ok', transaction_id)
                future_obj.set_result(op_result)
