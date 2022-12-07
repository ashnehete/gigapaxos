package edu.umass.cs.reconfiguration.examples.hello;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.interfaces.RequestCallback;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ActiveReplicaError;
import edu.umass.cs.reconfiguration.reconfigurationpackets.CreateServiceName;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static edu.umass.cs.reconfiguration.examples.hello.HelloAppRequest.PacketType.*;

public class HelloAppClient extends ReconfigurableAppClientAsync<Request> {

    public HelloAppClient() throws IOException {
        super();
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        try {
            return HelloApp.staticGetRequest(stringified);
        } catch (RequestParseException | JSONException ignored) {

        }
        return null;
    }


    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        HelloAppRequest.PacketType[] types = HelloAppRequest.PacketType.values();
        return new HashSet<IntegerPacketType>(Arrays.asList(types));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final HelloAppClient client = new HelloAppClient();

        int rand = (int) (Math.random() * Integer.MAX_VALUE);
        String name = "name_" + rand;
        String initialState = "default_data";
        client.sendRequest(new CreateServiceName(name, initialState),
                new RequestCallback() {
                    @Override
                    public void handleResponse(Request response) {
                        try {
                            if (response instanceof CreateServiceName && !((CreateServiceName) response).isFailed()) {
                                client.sendTestRequests(name);
                                System.out.println("Exit CreateServiceName");
                            } else {
                                System.out.println("Failed to create name: " + name);
                            }
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });

        Thread.sleep(1000);
//        client.close();
        System.out.println("Exit main");
    }

    private void sendTestRequests(String name) throws IOException, InterruptedException {
        System.out.println("Sending test requests for name: " + name);
        try {
            int rand = (int) (Math.random() * Integer.MAX_VALUE);
            System.out.println("Sending write request for name: " + name);
            HelloAppClient.this.sendRequest(
                    ReplicableClientRequest.wrap(new HelloAppRequest(name, "text_" + rand, WRITE_REQUEST)),
                    new RequestCallback() {
                        @Override
                        public void handleResponse(Request response) {
                            if (response instanceof ActiveReplicaError)
                                return;

                            System.out.println("Received response: "
                                    + response);
                        }
                    });

            Thread.sleep(1000);

            rand = (int) (Math.random() * Integer.MAX_VALUE);
            System.out.println("Sending append request for name: " + name);
            HelloAppClient.this.sendRequest(
                    ReplicableClientRequest.wrap(new HelloAppRequest(name, "text_" + rand, APPEND_REQUEST)),
                    new RequestCallback() {
                        @Override
                        public void handleResponse(Request response) {
                            if (response instanceof ActiveReplicaError)
                                return;

                            System.out.println("Received response: "
                                    + response);
                        }
                    });

            Thread.sleep(1000);

            System.out.println("Sending delete request for name: " + name);
            HelloAppClient.this.sendRequest(
                    ReplicableClientRequest.wrap(new HelloAppRequest(name, "text_" + rand, DELETE_REQUEST)),
                    new RequestCallback() {
                        @Override
                        public void handleResponse(Request response) {
                            if (response instanceof ActiveReplicaError)
                                return;

                            System.out.println("Received response: "
                                    + response);
                        }
                    });

            System.out.println("Exit sendTestRequests");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
