import grpc
from org.somda.protosdc.proto.model import sdc_services_pb2_grpc


class ArchiveService(sdc_services_pb2_grpc.ArchiveServiceServicer):
    def __init__(self, mdib):
        super().__init__()
        self._mdib = mdib

    def GetDescriptorsFromArchive(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetStatesFromArchive(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')
