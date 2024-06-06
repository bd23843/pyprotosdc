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