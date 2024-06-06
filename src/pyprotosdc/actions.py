from enum import Enum

_ActionsNamespace = 'org.somda.protosdc.mdib_reporting.action'

class ReportAction(str,Enum):
    Waveform = 'org.somda.protosdc.mdib_reporting.action.WaveformStreamMsg'
    DescriptionModificationReport = 'org.somda.protosdc.mdib_reporting.action.DescriptionModificationReport'
    EpisodicMetricReport = 'org.somda.protosdc.mdib_reporting.action.EpisodicMetricReport'
    EpisodicAlertReport = 'org.somda.protosdc.mdib_reporting.action.EpisodicAlertReport'
    EpisodicContextReport = 'org.somda.protosdc.mdib_reporting.action.EpisodicContextReport'
    EpisodicComponentReport = 'org.somda.protosdc.mdib_reporting.action.EpisodicComponentReport'
    EpisodicOperationalStateReport = 'org.somda.protosdc.mdib_reporting.action.EpisodicOperationalStateReport'

# this is a dummy action, only used internally for subscription manager
OperationInvokedAction = 'org.somda.protosdc.mdib_reporting.action.OperationInvokedReport'


class GetAction(str, Enum):
    GetMdibRequest = 'org.somda.protosdc.get.action.GetMdibRequest'
    GetMdibResponse = 'org.somda.protosdc.get.action.GetMdibResponse'
    GetMdDescriptionRequest = 'org.somda.protosdc.get.action.GetMdDescriptionRequest'
    GetMdDescriptionResponse = 'org.somda.protosdc.get.action.GetMdDescriptionResponse'
    GetMdStateRequest = 'org.somda.protosdc.get.action.GetMdStateRequest'
    GetMdStateResponse = 'org.somda.protosdc.get.action.GetMdStateResponse'
    GetContextStateRequest = 'org.somda.protosdc.get.action.GetContextStateRequest'
    GetContextStateResponse = 'org.somda.protosdc.get.action.GetContextStateResponse'