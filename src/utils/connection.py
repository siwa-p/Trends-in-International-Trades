from pyarrow import flight
class DremioConnection:
    def __init__(self, user, password, host, port):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.location = f"grpc+tcp://{self.host}:{self.port}"

    def connect(self):
        self.client = flight.FlightClient(self.location)
        bearer = self.client.authenticate_basic_token(self.user, self.password)
        self.options = flight.FlightCallOptions(headers=[bearer])
        print(f"Connected to Dremio at {self.location}")
        return self.client
    
    def query(self, query, client):
        flight_info = client.get_flight_info(
            flight.FlightDescriptor.for_command(query),
            self.options
        )
        results = client.do_get(flight_info.endpoints[0].ticket, self.options)
        return results