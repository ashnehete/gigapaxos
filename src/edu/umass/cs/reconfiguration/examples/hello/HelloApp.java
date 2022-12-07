package edu.umass.cs.reconfiguration.examples.hello;

import edu.umass.cs.gigapaxos.interfaces.ClientMessenger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.SSLMessenger;
import edu.umass.cs.reconfiguration.examples.AbstractReconfigurablePaxosApp;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class HelloApp extends AbstractReconfigurablePaxosApp implements Replicable, Reconfigurable, ClientMessenger {

    public HelloApp() {
        super();
    }

    public HelloApp(String[] args) {
    }

    private String myID; // used only for pretty printing
    private SSLMessenger<?, JSONObject> messenger;

    private class AppData {
        final String name;
        String state = "";

        AppData(String name, String state) {
            this.name = name;
            this.state = state;
        }

        void setState(String state) {
            this.state = state;
        }

        void appendState(String state) {
            this.state = this.state + state;
        }

        String getState() {
            return this.state;
        }

        @Override
        public String toString() {
            return "AppData{" +
                    "name='" + name + '\'' +
                    ", state='" + state + '\'' +
                    '}';
        }
    }

    HashMap<String, AppData> appData = new HashMap<String, AppData>();

    public static HelloAppRequest staticGetRequest(String stringified)
            throws RequestParseException, JSONException {
        return new HelloAppRequest(new JSONObject(stringified));
    }

    @Override
    public HelloAppRequest getRequest(String stringified) throws RequestParseException {
        try {
            return staticGetRequest(stringified);
        } catch (JSONException e) {
            System.out.println("App-" + myID + " unable to parse request " + stringified);
            throw new RequestParseException(e);
        }
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        HelloAppRequest.PacketType[] types = HelloAppRequest.PacketType.values();
        return new HashSet<IntegerPacketType>(Arrays.asList(types));
    }

    @Override
    public boolean execute(Request request) {
        return this.execute(request, false);
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        System.out.println("Executing request: " + request);
        HelloAppRequest.PacketType type = (HelloAppRequest.PacketType) request.getRequestType();

        HelloAppRequest req = (HelloAppRequest) request;

        if (req.getServiceName() == null)
            return true;

        if (req.isStop())
            return true;

        AppData data = this.appData.get(req.getServiceName());

        if (data == null) {
            System.out.println("App-" + myID + " has no record for "
                    + request.getServiceName() + " for " + request);
            return false;
        }

        switch (type) {
            case WRITE_REQUEST -> {
                data.setState(req.getText());
                this.appData.put(req.getServiceName(), data);
                System.out.println("App-" + myID + " WRITE to " + data.name + " with state " + data.getState());
            }
            case APPEND_REQUEST -> {
                data.appendState(req.getText());
                this.appData.put(req.getServiceName(), data);
                System.out.println("App-" + myID + " APPEND to " + data.name + " with state " + data.getState());
            }
            case DELETE_REQUEST -> {
                this.appData.remove(req.getServiceName());
                System.out.println("App-" + myID + " DELETE to " + data.name + " with state " + data.getState());
            }
        }

        req.setResponse(data.state);
        System.out.println("Set Response to: " + req.getResponse());

        return true;
    }

    @Override
    public String checkpoint(String name) {
        System.out.println("App-" + myID + " Checkpoint for " + name);
        AppData data = this.appData.get(name);
        return data != null ? data.getState() : null;
    }

    @Override
    public boolean restore(String name, String state) {
        System.out.println("App-" + myID + " Restore for " + name + " - " + state);
        AppData data = this.appData.get(name);

        if (data == null && state != null) {
            data = new AppData(name, state);
            System.out.println("App-" + myID + " Creating data: " + data.toString());
        } else if (state == null) {
            this.appData.remove(name);
            System.out.println("App-" + myID + " Removing data: " + name);
        } else if (data != null && state != null) {
            data.state = state;
            System.out.println("App-" + myID + " Update data: " + data.toString());
        } else
            // do nothing when data==null && state==null
            ;
        if (state != null)
            this.appData.put(name, data);

        return true;
    }

    @Override
    public void setClientMessenger(SSLMessenger<?, JSONObject> messenger) {
        this.messenger = messenger;
        this.myID = messenger.getMyID().toString();
    }

    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        return null;
    }
}
