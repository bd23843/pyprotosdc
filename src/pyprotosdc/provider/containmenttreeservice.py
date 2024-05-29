from org.somda.sdc.proto.model import sdc_services_pb2_grpc


class ContainmentTreeService(sdc_services_pb2_grpc.ContainmentTreeServicer):

    def __init__(self, mdib):
        super().__init__()
        self._mdib = mdib
